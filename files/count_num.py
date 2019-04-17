import os

path = "./20190416/"
file_Iterator = os.walk(path)
num = 0
for item in file_Iterator:
    print(item)
    for file_name in item[2]:
        with open("%s%s"%(path,file_name), 'r') as f:
            read_all = f.readlines()
            for i in read_all:
                if i :
                    num +=1
print(num)
# for item in file_Iterator:
#     for file_name in item[1]:
#         num = 0
#         files_Iterator = os.walk("%s/%s"%(path,file_name))
#         for i in files_Iterator:
#             for time in i[1]:
#                 time_Iterator = os.walk("%s/%s/%s" % (path, file_name,time))
#                 for t in time_Iterator:
#                     for file in t[2]:
#                         print("%s/%s/%s/%s"%(path,file_name,time,file))
#                         with open("%s/%s/%s/%s"%(path,file_name,time,file),'r') as f:
#
#                             read_all = f.readlines()
#                             num += len(read_all)
#
#         re_dic[file_name] = num
# print(re_dic)