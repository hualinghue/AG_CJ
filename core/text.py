import re
with open("201904110000.xml","rb+") as e:
    for i in e.readlines():
        req_dic = {}
        re_list = re.findall(' .*?=".*?"',i)
        for j in re_list:
            key, value = j.split("=")
            req_dic[key.strip()] = value.strip('"')