import MeCab
from pymongo import MongoClient, DESCENDING

def setup_mongo(db_name):
  connection = MongoClient()
  db = connection[db_name]
  print('mongoDB ready')
  return db

def setup_mecab(dic_path):
  mecab = MeCab.Tagger('-d ' + dic_path)
  mecab.parse('')
  print('mecab ready')
  return mecab
