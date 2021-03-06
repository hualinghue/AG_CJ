import ftplib
import datetime,json,re,os
from conf import settings
from core import log_handle
from pymongo import MongoClient

class Collect(object):
    def __init__(self,sys_args):
        self.sys_args=sys_args
        self.last_time = 0
        self.old_time = (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime("%Y%m%d")
        self.command_allowcator()
    def command_allowcator(self):
        '''分检用户输入的不同指令'''
        if len(self.sys_args)<3:
            print("缺少参数")
            return
        elif self.sys_args[1] == "start":
            self.forever_run()
        elif self.sys_args[1].isupper():
            collect_obj = Collect_handle(self.sys_args[2])
            collect_obj.proofread(time=self.sys_args[2],site_name=self.sys_args[1])
        else:
            print("参数1错误")
    def forever_run(self):
        while True:
            if datetime.datetime.now().timestamp() - self.last_time > settings.cj_interval:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"   开始采集")
                self.now_time = (datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d")
                collect_obj = Collect_handle(self.now_time)
                if self.old_time !=self.now_time:
                    self.old_time = self.now_time
                    collect_obj.proofread(time=self.old_time)
                else:
                    collect_obj.handle()
                self.last_time = datetime.datetime.now().timestamp()


class Collect_handle(object):
    def __init__(self,now_time):
        self.logs = log_handle.Log_handle()
        self.now_time = now_time
        self.DATA_TYPE = settings.DATA_TYPE
        self.link_ftp()
        self.link_mongo()
        self.get_all_site_name()
    def handle(self):
        for site_name in self.all_site_name:
            time_list = self.get_ftp_path_file_name("/" + site_name)
            if self.now_time in time_list:
                if time_list[-1] =="lostAndfound":
                    print("============lostAndfound=================")
                    lost_time_list = self.get_ftp_path_file_name("/%s/lostAndfound"%site_name)
                    for i in lost_time_list:
                        self.collect("%s/lostAndfound"%site_name, i)  # 采集
                    print("============lostAndfound=================")
                self.collect(site_name, self.now_time)  # 采集
    def collect(self, site_name, time,proofread=False):
        file_list = self.get_ftp_path_file_name("/%s/%s"%(site_name,time))
        if not file_list:
            return False
        if len(file_list) > settings.VALUE_NUM and not proofread:
            file_list = file_list[-settings.VALUE_NUM:]
        for file in file_list:
            date_list = self.download_file(file,site_name,time)    #下载
            self.write_mongo(date_list,site_name,file,time,proofread=proofread) if date_list else self.proofread(time,site_name)
    def get_last_time(self):
        #获取上次执行的文件名
        with open("../conf/last_time.txt") as f:
            site_obj = json.loads(f.read())
        return  site_obj
    def update_last_time(self,data_obj):
        #写入最后执行的文件名
        with open("../conf/last_time.txt",'w') as f:
            f.write(json.dumps(data_obj))
    def link_ftp(self):
        #连接ftp
        self.ftp = ftplib.FTP()
        try:
            self.ftp.connect(settings.host, settings.port, settings.timeout)
            self.ftp.login(settings.userName, settings.passWord)
            print("连接FTP成功")
        except Exception as e:
            print("连接FTP失败")
            print(e)
            self.logs.write_err( "连接FTP失败",self.now_time)
    def link_mongo(self):
        """连接mongo"""
        user = settings.DB_USER
        pwd = settings.DB_PASSWORD
        server = settings.DB_HOST
        port = settings.DB_PORT
        db_name = settings.DB_NAME
        # url = 'mongodb://%s:%s@%s:%s/%s' % (user, pwd, server, port, db_name)
        mongo_client = MongoClient(host=server,port=port)
        db = mongo_client[db_name]
        try:
            db.authenticate(user,pwd)
            self.mongo_obj = db
            print("连接mongo成功")
        except Exception as e:
            print('连接mongo失败',e)
            self.logs.write_err("连接mongo失败",self.now_time)
    def get_ftp_path_file_name(self, path):
        """获取FTP内容"""
        try:
            try:
                self.ftp.cwd("/")
                self.ftp.cwd(path)
                re_list = self.ftp.nlst()
            except (ftplib.error_proto,ftplib.error_perm) as e:
                self.logs.write_err("FTP:获取%s路径下的文件失败" % path,self.now_time)
                print("FTP:获取%s路径下的文件失败"%path,e)
                self.ftp.close()
                self.link_ftp()
                self.ftp.cwd(path)
                re_list = self.ftp.nlst()
            return re_list
        except Exception as e:
            self.ftp.close()
            self.link_ftp()
            self.get_all_site_name()
            self.handle()
            return []
    def proofread(self,time,site_name="ALL"):
        print("校队%s-%s" % (site_name,time))
        if site_name == "ALL":
            for site in self.all_site_name:
                time_list = self.get_ftp_path_file_name("/" + site)
                if time in time_list:
                    self.collect(site, time,proofread=True)  # 采集
                print("%s中没有%s文件"%(site,time))
        else:
            time_list = self.get_ftp_path_file_name("/" + site_name)
            if time not in time_list or site_name not in self.all_site_name:
                self.logs.write_err("%s中无数据"%site_name,time)
                return
            self.collect(site_name, time,proofread=True)  # 采集
    def download_file(self,file_name,site_name,time):
        ##下载FTP文件
        self.ftp.cwd("/%s/%s"%(site_name,time))
        file_path = "../files/%s/%s" % (site_name,time)
        if not os.path.exists(file_path):       #判断文件夹是否存在
            os.makedirs(file_path)
        with open("%s/%s"%(file_path,file_name),"wb") as f:
            print("下载/%s/%s"%(site_name,file_name))
            try:
                self.ftp.retrbinary("RETR %s"%file_name,f.write,1024)
            except Exception :
                self.handle()
                return False
        with open("%s/%s" % (file_path, file_name), "r") as f:
            file_line = f.readlines()       #查看下载是否成功
            print(file_line,file_name)
            if file_line:
                return self.analyze_xml(file_line)
            else:
                print("%s下载失败"%file_name)
                self.logs.write_err("%s下载失败"%file_name,time)
                return False
    def analyze_xml(self,file_list):
        ##解析xml数据
        re_list = []
        for line in file_list:
            req_dic = {}
            tmp_list = re.findall(' .*?=".*?"', str(line))
            for j in tmp_list:
                key, value = j.split("=")
                req_dic[key.replace(' ', '')] = value.strip('"')
            re_list.append(req_dic)
        return re_list
    def write_mongo(self,date_list,site_name,file_name,time,proofread=False):
        #写入mongo
        print("mongo准备写入%s/%s"% (site_name, file_name))
        judge_run = False
        for date in date_list:
            web_num = self.get_web_num(date["playerName"])      #获取网站编码
            if not web_num:
                print("%s/%s中%s解析失败" % (site_name,file_name,date["playerName"]))
                if proofread:
                    self.logs.proofread_err("%s/%s中%s解析失败" % (site_name,file_name,date["playerName"]),time)
                else:
                    self.logs.write_err("%s/%s中%s解析失败" % (site_name, file_name, date["playerName"]),time)
                continue
            elif web_num.isalpha():
                continue
            dataType_obj = self.DATA_TYPE[date["dataType"]]         #获取数据类型对象
            for itme in dataType_obj["change"]:        #数据转换
                change_data = date[itme]
                if not change_data:continue
                elif change_data == "null":
                    date[itme] = 0
                elif change_data == "type" or change_data == "flag":
                    date[itme] = int(change_data)
                else:
                    date[itme] = float(change_data)
            playformType = date["platformType"]
            table_name = "AG_%s_%s" % (date["dataType"], web_num)  # 拼接集合表名
            if date["dataType"] =="BR":
                if playformType == "YOPLAY":
                    table_name = "AG_YOBR_%s" % web_num  # 拼接集合表名
                elif date["platformType"] =="BBIN" and date['gameType'].startswith('5'):
                    table_name = "AG_EBR_%s" %  web_num
                elif date["platformType"] =="MG" and not date['gameType'].startswith("Live Games"):
                    table_name = "AG_EBR_%s" % web_num
                elif date["platformType"] =="PT":
                    dbobj = self.mongo_obj["AG_gameType"]
                    num = dbobj.count({'plat':'PT','type':'egame','code':date['gameType']})
                    if num >0:
                        table_name = "AG_EBR_%s" % web_num
                    else:
                        table_name = "AG_BR_%s" % web_num

            # only_ID = date[dataType_obj["type"]]                            #获取数据的唯一键
            table_obj = self.mongo_obj[table_name]
            if table_obj.count() == 0:              #获取索引
                table_obj.ensure_index(dataType_obj["type"], unique=True)
            MDtime = date[dataType_obj["time"]] or date["betTime"]        #获取时间
            try:
                BJtime = datetime.datetime.strptime(MDtime, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=12)
                date["bjTime"] = BJtime.strftime('%Y-%m-%d %H:%M:%S')  # 添加北京时间

            except Exception :
                print("======================================"+MDtime+"+++++++++++++++++++")
                print(date)
            try:
                judge_run = True
                table_obj.insert(date)        #写入数据
            except Exception as e:
                pass
            # if not table_obj.find_one({dataType_obj["type"]:only_ID}):       #查询库中是否存在
            #     if not table_obj.insert(date):
            #         print("mongo：%s/%s写入%s:%s失败" % (site_name,table_name, dataType_obj["type"], only_ID))
            #         if proofread:
            #             self.logs.proofread_err("mongo：%s写入%s:%s失败" % (table_name, dataType_obj["type"], only_ID),time)
            #         else:
            #             self.logs.write_err("mongo：%s写入%s:%s失败" % (table_name, dataType_obj["type"], only_ID),time)
            #     else:
            #
            #         judge_run = True
            #         if proofread:
            #             self.logs.proofread_acc("mongo：%s写入%s:%s成功" % ( table_name, dataType_obj["type"], only_ID),time)
            #         else:
            #             self.logs.write_acc("mongo：%s写入%s:%s成功" % (table_name, dataType_obj["type"], only_ID),time)
        if not judge_run:
            print("无数据写入")
        print("%s/%s文件执行完成" % (site_name, file_name))
    def get_web_num(self,username):
        req_name = re.search(r"[m|M]12(\d\d\d)",username) or re.search(r"[m|M]12(hg|HG)",username)
        if req_name:
            return req_name.group(1)
        else:
            return None
    def get_all_site_name(self):
        """获取所有平台的名字"""
        self.all_site_name =  self.get_ftp_path_file_name("/")

def analyze_xml(file_list):
    ##解析xml数据
    re_list = []
    for line in file_list:
        req_dic = {}
        tmp_list = re.findall(' .*?=".*?"', str(line))
        for j in tmp_list:
            key, value = j.split("=")
            req_dic[key.replace(' ', '')] = value.strip('"')
        re_list.append(req_dic)
    return re_list
def get_web_num(username):
    req_name = re.search(r"[m|M]12(\d\d\d)",username) or re.search(r"[m|M]12(hg|HG)",username)
    if req_name:
        return req_name.group(1)
    else:
        return None
a=['<row dataType="BR"  billNo="190523237021970" playerName="M12002mp5678" agentCode="A0B001001001001" gameCode="GB002195231F6" netAmount="50" betTime="2019-05-23 23:50:33" gameType="BAC" betAmount="50" validBetAmount="50" flag="1" playType="2" currency="CNY" tableCode="B002" loginIP="117.136.8.15" recalcuTime="2019-05-23 23:50:48" platformType="AGIN" remark="" round="AGQ" result="" beforeCredit="231" deviceType="1" />']
b=analyze_xml(a)
c=get_web_num(b[0]['playerName'])

print(c)
