import os
from . import AGcollect

class Proofread(AGcollect):
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
                    self.write_mongo(file_lines_list, site_name, file_name)
            else:
                self.download_file(file_name,site_name,time)

