VALUE_NUM=9

cj_interval = 120            #监控间隔
host = 'xm.gdcapi.com'      #ftp采集地址
userName = 'M12.aomenyinhe'   #账号
passWord = 'BJvX#paU8F'       #密码
port = 21
timeout = 100


DB_USER = "cj_man"
DB_PASSWORD = "sSDm_lizdmOggz"
DB_PORT = 27017
DB_NAME = "video_cj"
DB_HOST = "10.8.63.117"

DATA_TYPE = {
        "BR":{"type":"billNo", "time":"recalcuTime","change":["betAmount","validBetAmount","netAmount","flag"]},   #下注记录
        "HSR":{"type":"tradeNo","time":"creationTime","change":["type","Cost","Earn","flag"]},   #捕鱼王转账记录
        "HPR":{"type":"tradeNo","time":"creationTime","change":["transferAmount","flag"]},  #捕鱼王养鱼游戏记录
        "EBR":{"type":"billNo", "time":"recalcuTime","change":["betAmount","validBetAmount","netAmount","flag"]},   #电子游戏下注记录
        "TR":{"type":"transferId","time":"creationTime","change":["transferAmount","flag"]},  #户口转账记录
        "GR":{"type":"gmcode","time":"closetime","change":["flag"]},           #游戏结果"GR"仅导出AGIN平台的真人视讯游戏相关数据
        "HTR":{"type":"transferId","time":"creationTime","change":["transferAmount","flag"]},
}
