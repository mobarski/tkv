# !!! PRE-ALPHA AS HELL - DO NOT USE IT !!!

import tkv
import pymongo

# TODO: loads dumps
# TODO: scan_xxx

class TKVmongo(tkv.TKV):
	
	def __init__(self, conn_str, db='tkv'):
		self.db = pymongo.MongoClient(conn_str)[db]
	
	# core
	
	def put(self, tab, key, val):
		self.db[tab].replace_one({'_id':key},{'_id':key, 'val':val}, upsert=True)
		
	def get(self, tab, key, default=None):
		try:
			return self.db[tab].find_one(key)['val']
		except TypeError:
			return default
		
	def has(self, tab, key):
		return bool(self.db[tab].find_one(key,['_id']))
	
	def delete(self, tab, key):
		self.db[tab].delete_one({'_id':key})
	
	def drop(self, tab):
		self.db[tab].drop()
	
	def size(self, tab):
		try:
			return self.db.command('collstats', tab)['size'] # TODO: use storegeSize?
		except pymongo.errors.OperationFailure:
			return 0
		
	# iterators
	
	def keys(self, tab, sort=False):
		docs = self.db[tab].find({},[])
		return (x['_id'] for x in docs)
		
	def items(self, tab, sort=False):
		docs = self.db[tab].find({},[])
		return ((x['_id'],x['val']) for x in docs)

	# extension

	def get_many(self, tab, keys, default=None):
		docs = self.db[tab].find({'_id':{'$in':keys}},['val'])
		docs = {x['_id']:x['val'] for x in docs}
		return [docs.get(k,default) for k in keys]

	def put_items(self, tab, items):
		batch = [pymongo.ReplaceOne({'_id':k}, {'_id':k,'val':v}, upsert=True) for k,v in items]
		self.db[tab].bulk_write(batch)

	def count(self, tab, sort=False):
		return self.db[tab].count_documents({})
	
	# other
	
	def tables(self):
		return [x['name'] for x in self.db.list_collections()]

def connect(*a,**kw):
	return TKVmongo(*a,**kw)
