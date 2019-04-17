import os

path = "../files/"
file_Iterator = os.walk(path)
re_dic = {}
download_file_list = []
for item in file_Iterator:
    download_file_list = item[2]
    break
for file_name in download_file_list:
    num = 0
    files_Iterator = os.walk("%s/%s"%(path,file_name))
    for i in files_Iterator:
        for time in i[1]:
            time_Iterator = os.walk("%s/%s/%s" % (path, file_name,time))
            for t in time_Iterator:
                for file in t[2]:
                    with open("%s/%s/%s/%s"%(path,file_name,time,file),'r') as f:
                        for i in f.readlines():
                            if i != "run_ok":
                                num += 1
    re_dic[file_name] = num
print(re_dic)