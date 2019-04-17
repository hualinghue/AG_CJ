import os

class Proofread(object):
    def __init__(self,ftp_obj,mongo_obj):
        self.ftp = ftp_obj
        self.mongo = mongo_obj
    def handle(self,time,site_name):
        path = "../files/%s/%s" %(site_name,time)
        file_Iterator = os.walk(path)
        download_file_list = []
        for item in file_Iterator:
            download_file_list = item[1]
        self.ftp.cwd("/%s/%s" % (site_name, time))
        file_list = self.ftp.nlst()
        for file_name in file_list:
            if file_name in download_file_list:
                with open(path,'r') as f:
                    file_lines_list = f.readlines()
                    db_date_id = self.mongo[site_name].find_one(file_lines_list[0])
                    if not db_date_id:
                        aa= self.mongo.insert(file_lines_list)
                        print("mongo写入%s/%s  %s  %s" % (site_name, file_name,len(file_lines_list),len(aa)))
            else:
                print("没有%s这个文件"%file_name)

