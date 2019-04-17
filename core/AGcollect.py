import ftplib
import datetime,json,re,os
from conf import settings
from . import log_handle
from pymongo import MongoClient

class Collect(object):
    def __init__(self):
        self.logs = log_handle.Log_handle()
        self.link_mongo()
        self.last_time = 0
    def forever_run(self):
        while True:
            if datetime.datetime.now().timestamp() - self.last_time > settings.cj_interval:
                print(datetime.datetime.now().timestamp(),"   开始采集")
                self.link_ftp()
                self.now_time = (datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d")
                self.collect_handle()
                self.last_time = datetime.datetime.now().timestamp()
                self.ftp.close()
    def collect_handle(self):
        #采集
        allFileName = self.ftp.nlst()  # //列举出远程FTP下的文件夹的名字
        site_obj = self.get_last_time()   #获取上次执行过的文件名
        for lists in allFileName:
            self.ftp.cwd("/" + lists)  # 进入目录
            time_list = self.ftp.nlst()
            if self.now_time in time_list:
                self.ftp.cwd("/%s/%s" % (lists,self.now_time))
                site_list = self.collect_list(self.ftp.nlst(),lists,site_obj)   #筛选出需要下载的文件名
                if site_list:
                    for file_name in site_list:
                        self.ftp.cwd("/%s/%s" % (lists, self.now_time))
                        val_list = self.download_file(file_name,lists,self.now_time)     #下载文件
                        if val_list:
                            self.write_mongo(val_list,lists,file_name)             #写入mongo
                    else:
                        site_obj[lists] = file_name         #最后一个文件名存入文件中
        self.update_last_time(site_obj)
        self.ftp.close()
    def collect_list(self,all_file_name,site_name,site_obj):
        ##返回需要下载的文件列表
        last_file_name = site_obj[site_name]
        if last_file_name in all_file_name:
            return all_file_name[all_file_name.index(last_file_name)+1:]
        else:
            if last_file_name:
                self.proofread(last_file_name, site_name)   #校队
            return all_file_name
    def download_file(self,file_name,site_name,time):
        ##下载FTP文件
        bufsize = 1024
        re_list = []
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
    def write_mongo(self,date_list,site_name,file_name):
        #写入mongo
        table_obj = self.mongo_obj[site_name]
        for data in date_list:
            if site_name == "AGIN":
                db_date_id = table_obj.find_one({"billNo":data["billNo"]})
            else:
                db_date_id = table_obj.find_one(data)
            if not db_date_id:
                aa = table_obj.insert(date_list)
                print("mongo写入%s/%s"%(site_name,file_name,),len(date_list),len(aa))
                self.logs.write_acc({"title": "mongo写入%s/%s  %s  %s"%(site_name,file_name,len(date_list),len(aa)), "data": "ok"})
            else:
                self.logs.write_err({"title": "mongo:%s/%s  %s已存在"%(site_name,file_name,data)})
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
    def proofread(self,file_name,site_name):
        #上一次执行的文件不再今日文件列表进行校队
        print("校队：/%s/%s"%(site_name,file_name))
        last_time = file_name[0:8]     #获取文件日期
        self.ftp.cwd("/"+site_name)
        timePT_list = self.ftp.nlst()
        if last_time in timePT_list:
            self.ftp.cwd("/%s/%s"%(site_name,last_time))
            file_list = self.ftp.nlst()
            if file_name in file_list:
                over_file = file_list[file_list.index(file_name)+1:]
                for itme in over_file:
                    val_list = self.download_file(itme,site_name,last_time)
                    if val_list:
                        self.write_mongo(val_list,site_name,itme)
            last_to_now = timePT_list[timePT_list.index(last_time)+1:timePT_list.index(self.now_time)]   #获取上一次执行日期到今天之间的日期
            for item in last_to_now:
                self.ftp.cwd("/%s/%s" % (site_name, item))
                self.logs.write_err({"title": "%s  %s" % (item, self.ftp.nlst())})
                for i in self.ftp.nlst():
                    val_list = self.download_file(i, site_name, item)
                    if val_list:
                        self.write_mongo(val_list, site_name, i)
    def _proofread(self, time, site_name):
        od = ['201904160620.xml','201904160622.xml']
        self.link_ftp()
        path = "../files/%s/%s" % (site_name, time)
        file_Iterator = os.walk(path)
        download_file_list = []
        for item in file_Iterator:
            download_file_list = item[2]
        self.ftp.cwd("/%s/%s" % (site_name, time))
        file_list = self.ftp.nlst()
        for file_name in file_list:
            if file_name in download_file_list:
                if file_name in od:
                    with open("%s/%s"%(path,file_name), 'r') as f:
                        file_lines_list = self.analyze_xml(f.readlines())
                        print(len(file_lines_list))
                        if file_lines_list:
                            self.write_mongo(file_lines_list, site_name, file_name)
            # else:
            #     file_list = self.download_file(file_name, site_name, time)
            #     if file_list:
            #         self.write_mongo(file_list, site_name, file_name)


