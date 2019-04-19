import datetime
class Log_handle(object):
    def write_err(self,date):
        self.write("../logs/error%s.log"%(datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d"),date)
    def write_acc(self,date):
        self.write("../logs/access%s.log"%(datetime.datetime.now() - datetime.timedelta(hours=12)).strftime("%Y%m%d"),date)
    def write_repeat(self,date):
        self.write("../logs/repeat%s.log"%(datetime.datetime.now() - datetime.timedelta(hours=12)).strftime("%Y%m%d"),date)
    def write(self,file_name,date):
        with open(file_name,"a+") as f:
            f.write("%s  %s"%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),date))
            f.write('\n')