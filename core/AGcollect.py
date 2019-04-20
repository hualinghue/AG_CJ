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
                    self.collect(site_name, "lostAndfound")  # 采集
                self.collect(site_name, self.now_time)  # 采集
    def collect(self, site_name, time,proofread=False):
        file_list = self.get_ftp_path_file_name("/%s/%s"%(site_name,time))
        if not file_list:
            return False
        if len(file_list) > settings.VALUE_NUM and not proofread:
            file_list = file_list[-settings.VALUE_NUM:]
        for file in file_list:
            date_list = self.download_file(file,site_name,time)    #下载
            self.write_mongo(date_list,site_name,file,proofread=proofread) if date_list else self.proofread(time,site_name)
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
            self.logs.write_err( "连接FTP失败")
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
            self.logs.write_err("连接mongo失败")
    def get_ftp_path_file_name(self, path):
        """获取FTP内容"""
        try:
            self.ftp.cwd("/")
            self.ftp.cwd(path)
            re_list = self.ftp.nlst()
        except (ftplib.error_proto,ftplib.error_perm) as e:
            self.logs.write_err("FTP:获取%s路径下的文件失败" % path)
            print("FTP:获取%s路径下的文件失败"%path,e)
            self.ftp.close()
            self.link_ftp()
            self.ftp.cwd(path)
            re_list = self.ftp.nlst()
        return re_list
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
                raise print("%s中无数据"%site_name)
            self.collect(site_name, time,proofread=True)  # 采集
    def download_file(self,file_name,site_name,time):
        ##下载FTP文件
        self.ftp.cwd("/%s/%s"%(site_name,time))
        file_path = "../files/%s/%s" % (site_name,time)
        if not os.path.exists(file_path):       #判断文件夹是否存在
            os.makedirs(file_path)
        with open("%s/%s"%(file_path,file_name),"wb+") as f:
            print("下载/%s/%s"%(site_name,file_name))
            self.ftp.retrbinary("RETR %s"%file_name,f.write,1024)
            f.seek(0,0)
            file_line = f.readlines()       #查看下载是否成功
        if file_line:
            return self.analyze_xml(file_line)
        else:
            os.remove("%s/%s"%(file_path,file_name))
            print("%s下载失败"%file_name)
            self.logs.write_err("%s下载失败"%file_name)
            return False
    def analyze_xml(self,file_list):
        ##解析xml数据
        re_list = []
        for line in file_list:
            req_dic = {}
            tmp_list = re.findall(' .*?=".*?"', str(line))
            for j in tmp_list:
                key, value = j.split("=")
                req_dic[key.replace(' ','')] = value.strip('"')
            re_list.append(req_dic)
        return re_list
    def write_mongo(self,date_list,site_name,file_name,proofread=False):
        #写入mongo
        print("mongo准备写入%s/%s"% (site_name, file_name))
        judge_run = False
        for date in date_list:
            web_num = self.get_web_num(date["playerName"])      #获取网站编码
            if not web_num:
                print("%s/%s中%s解析失败" % (site_name,file_name,date["playerName"]))
                if proofread:
                    self.logs.proofread("%s/%s中%s解析失败" % (site_name,file_name,date["playerName"]))
                else:
                    self.logs.write_err("%s/%s中%s解析失败" % (site_name, file_name, date["playerName"]))
                continue
            elif web_num.islower():
                web_num.upper()
            dataType = self.DATA_TYPE[date["dataType"]]         #获取数据类型
            only_ID = date[dataType]                            #获取数据的唯一键
            table_name = "%s_%s_%s" %(site_name,date["dataType"],web_num)    #拼接集合表名
            table_obj = self.mongo_obj[table_name]
            if not table_obj.find_one({dataType:only_ID}):       #查询库中是否存在
                if not table_obj.insert(date):
                    print("mongo：%s/%s写入%s:%s失败" % (site_name,table_name, dataType, only_ID))
                    if proofread:
                        self.logs.proofread("mongo：%s写入%s:%s失败" % (table_name, dataType, only_ID))
                    else:
                        self.logs.write_err("mongo：%s写入%s:%s失败" % (table_name, dataType, only_ID))
                else:
                    judge_run = True
                    if proofread:
                        self.logs.proofread("mongo：%s/%s写入%s:%s成功" % (site_name, table_name, dataType, only_ID))
                    else:
                        self.logs.write_acc("mongo：%s/%s写入%s:%s成功" % (site_name,table_name, dataType, only_ID))
            else:
                print("%s/%s文件中的%s：%s重复" % (site_name,file_name, dataType, only_ID))
                if proofread:
                    self.logs.proofread("%s/%s文件中的%s：%s重复" % (site_name, file_name, dataType, only_ID))
                else:
                    self.logs.write_repeat("%s/%s文件中的%s：%s重复" % (site_name,file_name, dataType, only_ID))
        if not judge_run:print("无数据写入")
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

