import pymongo

myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")


mydb = myclient["aooooo"]
mycol = mydb["customers"]

mydict = { "name": "John", "address": "Highway 37" }

x = mycol.insert_one(mydict)
print(myclient.list_database_names())

