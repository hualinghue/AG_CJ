from pymongo import MongoClient

def write_mongo():
	user = "cj_man"
	pwd = "sSDm_lizdmOggz"
	server = "10.8.63.117"
	port = 27017
	db_name = "video_cj"
	# url = 'mongodb://%s:%s@%s:%s/%s' % (user, pwd, server, port, db_name)
	mongo_client = MongoClient(host=server,port=port)
	db = mongo_client[db_name]
	try:
		db.authenticate(user,pwd)
		mongo_obj = db
	except Exception as e:
		print('连接mongo失败',e)
write_mongo()
