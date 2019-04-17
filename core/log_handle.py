import datetime,json

class Log_handle(object):
    def write_err(self,date):
        with open("../logs/error%s.log"%(datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d"),"a+") as f:
            f.write("%s  %s"%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),json.dumps(date)))
            f.write('\n')
    def write_acc(self,date):
        with open("../logs/access%s.log"%(datetime.datetime.now()-datetime.timedelta(hours=12)).strftime("%Y%m%d"),"a+") as f:
            f.write("%s  %s"%(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),json.dumps(date)))
            f.write('\n')