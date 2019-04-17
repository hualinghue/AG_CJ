import ftplib
def link_ftp():
    #连接ftp
    ftp = ftplib.FTP()
    try:
        ftp.connect("xm.gdcapi.com", 21, 100)
        ftp.login("M12.aomenyinhe", 'BJvX#paU8F')
    except Exception as e:
        print("连接FTP失败")
        print(e)
        logs.write_err({"title": "连接FTP失败"})

link_ftp()





