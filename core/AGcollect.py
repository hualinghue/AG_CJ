import ftplib
import datetime,json,re,os
from conf import settings
from . import log_handle
from pymongo import MongoClient

class Collect(object):
    def __init__(self,sys_args):
        self.sys_args=sys_args
        self.logs = log_handle.Log_handle()
        self.link_mongo()
        self.link_ftp()
        self.last_time = 0
        self.command_allowcator()
    def command_allowcator(self):
        '''分检用户输入的不同指令'''
        if len(self.sys_args)<3:
            print("缺少参数")
            return
        download_file_list = self.get_ftp_path_file_name('/')
        print(download_file_list)
        if self.sys_args[1]  in download_file_list:
            self._proofread()
        elif self.sys_args[1] == "start":
            self.ftp.close()
            self.forever_run()
        else:
            self.ftp.close()
            print("参数1错误")
    def forever_run(self):
        while True:
            if datetime.datetime.now().timestamp() - self.last_time > settings.cj_interval:
                self.link_ftp()
                self.get_all_site_name()
                print("====",self.all_site_name,"=====")
                print(datetime.datetime.now().timestamp(),"   开始采集")
                self.now_time = (datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d")
                self.collect_handle()
                self.last_time = datetime.datetime.now().timestamp()
                self.ftp.close()
    def collect_handle(self):
        #采集
        allFileName = self.get_ftp_path_file_name("/")  # //列举出远程FTP下的文件夹的名字
        site_obj = self.get_last_time()   #获取上次执行过的文件名
        for lists in allFileName:
            time_list = self.get_ftp_path_file_name("/" + lists)
            if self.now_time in time_list:
                site_list = self.collect_list(
                    self.get_ftp_path_file_name("/%s/%s" % (lists,self.now_time))
                    ,lists,site_obj
                )   #筛选出需要下载的文件名
                if site_list:
                    for file_name in site_list:
                        val_list = self.download_file(file_name,lists,self.now_time)     #下载文件
                        if val_list:
                            self.write_mongo(val_list,lists,file_name,self.now_time)             #写入mongo
                    else:
                        site_obj[lists] = file_name         #最后一个文件名存入文件中
        self.update_last_time(site_obj)
    def collect_list(self,all_file_name,site_name,site_obj):
        ##返回需要下载的文件列表
        last_file_name = site_obj[site_name]
        if last_file_name in all_file_name:
            return all_file_name[all_file_name.index(last_file_name)+1:]
        else:
            return all_file_name
    def download_file(self,file_name,site_name,time):
        ##下载FTP文件
        bufsize = 1024
        file_path = "../files/%s/%s" % (site_name,time)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        try:
            with open("%s/%s"%(file_path,file_name),"wb+") as f:
                print("下载/%s/%s"%(site_name,file_name))
                self.logs.write_acc({"title": "下载/%s/%s"%(site_name,file_name), "data": "ok"})
                self.ftp.retrbinary("RETR %s"%file_name,f.write,bufsize)
                f.seek(0,0)
                re_list =self.analyze_xml(f.readlines())
        except Exception as e:
            print("下载文件%s失败"%file_name)
            print(e)
            self.logs.write_err({"title":"下载文件%s失败"%file_name})
            return False
        # os.remove("../files/%s"%file_name)
        return re_list
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
    def write_mongo(self,date_list,site_name,file_name,time):
        #写入mongo
        table_obj = self.mongo_obj[site_name]
        file_path = "../files/%s/%s/%s"%(site_name,time,file_name)
        with open(file_path,"r") as f:
            file_line = f.readline()
        if file_line[-1] == "run_ok":
            self.logs.write_err({"title": "mongo:%s已执行" % (file_path)})
        else:
            aa = table_obj.insert(date_list)
            print("mongo写入%s/%s共%s条"%(site_name,file_name,len(aa)))
            self.logs.write_acc({"title": "mongo写入%s"%(file_path), "data": len(aa)})
            with open(file_path, "a") as f:
                f.write("run_ok")

        # for data in date_list:
        #     if site_name == "AGIN":
        #         try:
        #             db_date_id = table_obj.find_one({"billNo":data["billNo"]})
        #         except KeyError as e:
        #             db_date_id = table_obj.find_one({"tradeNo": data["tradeNo"]})
        #     else:
        #         db_date_id = table_obj.find_one(data)
        #     if not db_date_id:
        #         aa = table_obj.insert(date_list)
        #         print("mongo写入%s/%s"%(site_name,file_name,),len(date_list),len(aa))
        #         self.logs.write_acc({"title": "mongo写入%s/%s  %s  %s"%(site_name,file_name,len(date_list),len(aa)), "data": "ok"})
        #     else:
        #         self.logs.write_err({"title": "mongo:%s/%s  %s已存在"%(site_name,file_name,data)})
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
            self.logs.write_err({"title": "连接FTP失败"})
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
            self.logs.write_err({"title": "连接mongo失败", "data": e})
    def get_last_time(self):
        #获取上次执行的文件名
        with open("../conf/last_time.txt") as f:
            site_obj = json.loads(f.read())
        return  site_obj
    def update_last_time(self,data_obj):
        #写入最后执行的文件名
        with open("../conf/last_time.txt",'w') as f:
            f.write(json.dumps(data_obj))
    def _proofread(self):
        """校队
            参数一：平台代号(AG..)
            参数二：校队日期
        """
        site_obj = self.get_last_time()
        time = self.sys_args[2]
        site_name = self.sys_args[1]
        path = "../files/%s/%s" % (site_name, time)
        download_file_list = self.get_path_file_name(path)
        file_list = self.get_ftp_path_file_name("/%s/%s" % (site_name, time))
        for file_name in file_list:
            if file_name in download_file_list:
                with open("%s/%s"%(path,file_name), 'r') as f:
                    file_lines = f.readlines()
                    if file_lines[-1] != "run_ok":
                        file_lines_list = self.analyze_xml(file_lines)
                        self.write_mongo(file_lines_list, site_name, file_name,time)
            else:
                file_list = self.download_file(file_name, site_name, time)
                if file_list:
                    self.write_mongo(file_list, site_name, file_name,time)
                    site_obj[site_name] = file_name
                else:
                    self.logs.write_err({"title": "文件下载有误",'data':file_list})
        self.update_last_time(site_obj)
    def get_path_file_name(self,path):
        """获取文件内容"""
        file_Iterator = os.walk(path)
        for item in file_Iterator:
            return item[2]
    def get_ftp_path_file_name(self, path):
        """获取FTP内容"""
        self.ftp.cwd("/")
        self.ftp.cwd(path)
        re_list = self.ftp.nlst()
        return re_list
    def get_all_site_name(self):
        """获取所有平台的名字"""
        self.all_site_name =  self.get_path_file_name("/")


