import ftplib
import datetime,json,re,os
from conf import settings
from core import log_handle
from pymongo import MongoClient

class Collect(object):
    def __init__(self,sys_args):
        self.sys_args=sys_args
        self.last_time = 0
        self.command_allowcator()
    def command_allowcator(self):
        '''分检用户输入的不同指令'''
        if len(self.sys_args)<3:
            print("缺少参数")
            return
        elif self.sys_args[1] == "start":
            self.forever_run()
        else:
            print("参数1错误")
    def forever_run(self):
        while True:
            if datetime.datetime.now().timestamp() - self.last_time > settings.cj_interval:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"   开始采集")
                self.now_time = (datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d")
                collect_obj = Collect_handle(self.now_time)
                collect_obj.handle()
                self.last_time = datetime.datetime.now().timestamp()


class Collect_handle(object):
    def __init__(self,now_time):
        self.logs = log_handle.Log_handle()
        self.now_time = now_time
        self.site_obj = self.get_last_time()  # 获取上次执行过的文件名
        self.DATA_TYPE = settings.DATA_TYPE
        self.link_ftp()
        self.link_mongo()
        self.get_all_site_name()
    def handle(self):
        for site_name in self.all_site_name:
            time_list = self.get_ftp_path_file_name("/" + site_name)
            if self.now_time not in time_list:
                self.proofread()        #校队
            else:
                self.collect("/%s/%s/"%(site_name,self.now_time),site_name)    #采集
            self.update_last_time(self.site_obj)
    def collect(self, path, site_name):
        file_list = self.get_ftp_path_file_name(path)
        last_file = self.site_obj[site_name]
        if last_file in file_list:
            file_list = file_list[file_list.index(last_file)+1:]    #过滤已执行的文件
        for file in file_list:
            date_list = self.download_file(file,site_name)    #下载
            self.write_mongo(date_list,site_name,file) if date_list else False
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
        self.ftp.cwd("/")
        self.ftp.cwd(path)
        try:
            re_list = self.ftp.nlst()
        except ftplib.error_proto as e:
            self.logs.write_err("FTP:获取%s路径下的文件失败" % path)
            print("FTP:获取%s路径下的文件失败",e)
            self.ftp.close()
            self.link_ftp()
            self.ftp.cwd(path)
            re_list = self.ftp.nlst()

        return re_list
    def proofread(self):
        pass
    def download_file(self,file_name,site_name):
        ##下载FTP文件
        file_path = "../files/%s/%s" % (site_name,self.now_time)
        if not os.path.exists(file_path):       #判断文件夹是否存在
            os.makedirs(file_path)
        with open("%s/%s"%(file_path,file_name),"wb+") as f:
            print("下载/%s/%s"%(site_name,file_name))
            # self.logs.write_acc("下载/%s/%s"%(site_name,file_name))
            self.ftp.retrbinary("RETR %s"%file_name,f.write,1024)
            f.seek(0,0)
            file_line = f.readlines()
            return self.analyze_xml(file_line) if file_line else self.logs.write_err("%s下载失败"%file_name)
    def analyze_xml(self,file_list,many=True):
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
    def write_mongo(self,date_list,site_name,file_name):
        #写入mongo
        print("mongo准备写入%s/%s"% (site_name, file_name))
        self.site_obj[site_name] = file_name
        judge_write = False
        for date in date_list:
            web_num = self.get_web_num(date["playerName"])      #获取网站编码
            dataType = self.DATA_TYPE[date["dataType"]]         #获取数据类型
            only_ID = date[dataType]                            #获取数据的唯一键
            table_name = "%s_%s_%s" %(site_name,date["dataType"],web_num)    #拼接集合表名
            table_obj = self.mongo_obj[table_name]
            if not table_obj.find_one({dataType:only_ID}):       #查询库中是否存在
                date["siteNo"] = web_num
                if table_obj.insert(date):
                    judge_write = True
                else:
                    print("mongo：%s/%s写入%s:%s失败" % (site_name,table_name, dataType, only_ID))
                    self.logs.write_err("mongo：%s写入%s:%s失败" % (table_name, dataType, only_ID))
            else:
                print("%s/%s文件中的%s：%s重复" % (site_name,file_name, dataType, only_ID))
                self.logs.write_repeat("%s/%s文件中的%s：%s重复" % (site_name,file_name, dataType, only_ID))
        if not judge_write:
            print("%s/%s文件以执行" % (site_name,file_name))
            self.logs.write_err("%s/%s文件以执行" % (site_name,file_name))
        else:
            print("%s/%s文件执行成功" % (site_name, file_name))
            self.logs.write_acc("%s/%s文件执行成功" % (site_name, file_name))
    def get_web_num(self,username):
        req_name = re.search(r"[m|M]12([A-Z]+)",username) or re.search(r"[m|M]12(\d\d\d)",username)
        if req_name:
            return req_name.group(1)
        else:
            self.logs.write_err("%s解析失败"%username)
            return "None"
    def get_all_site_name(self):
        """获取所有平台的名字"""
        self.all_site_name =  self.get_ftp_path_file_name("/")

