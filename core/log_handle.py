import datetime
class Log_handle(object):
    def proofread_err(self,date,time):
        self.write("../logs/proofread_err-%s.log" % time,date)
    def proofread_acc(self,date,time):
        self.write("../logs/proofread_acc-%s.log" % time,date)
    def write_err(self,date,time):
        self.write("../logs/error-%s.log"%time,date)
    def write_acc(self,date,time):
        self.write("../logs/access-%s.log"%time,date)
    def write_repeat(self,date,time):
        self.write("../logs/repeat-%s.log"%time,date)
    def write(self,file_name,date):
        with open(file_name,"a+") as f:
            f.write("%s  %s"%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),date))
            f.write('\n')
