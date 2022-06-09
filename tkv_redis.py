import tkv
import redis
import json

class TKVredis(tkv.TKV):
	"""Redis/SSDB/KeyDB/Ardb adapter"""
	
	def __init__(self, dumps=None, loads=None, **kw):
		self.db = redis.Redis(**kw)
		self.dumps = dumps or json.dumps
		self.loads = loads or json.loads
		self.encoding = kw.get('encoding','utf8')

	# core
	
	def put(self, tab, key, val):
		val = self.dumps(val) if self.dumps else val
		self.db.hset(tab, key, val)
		
	def get(self, tab, key, default=None):
		val = self.db.hget(tab, key)
		val = self.loads(val) if val else default
		return val
		
	def has(self, tab, key):
		return self.db.hexists(tab, key)
		
	def tables(self):
		return (x.decode(self.encoding) for x in self.db.keys())
		
	def drop(self, tab):
		self.db.delete(tab)
		
	def delete(self, tab, key):
		self.db.hdel(tab, key)

	def size(self, tab):
		try:
			resp = self.db.execute_command(f'DEBUG OBJECT {tab}').decode()
			for x in resp.split(' '):
				if x.startswith('serializedlength:'):
					return int(x.split(':')[1])
		except:
			return 0

	# iterators

	def keys(self, tab, sort=False):
		return [x.decode(self.encoding) for x in self.db.hkeys(tab)]
				
	def items(self, tab, sort=False):
		items = self.db.hgetall(tab)
		items = ((x[0].decode(self.encoding),self.loads(x[1])) for x in items)
		return items

	# extension

	def get_many(self, tab, keys, default=None):
		values = self.db.hmget(tab, keys)
		values = [self.loads(x) if x else default for x in values]
		return values
		
	def put_items(self, tab, items):
		self.db.hmset(tab, {k:self.dumps(v) for k,v in items})
		
	def count(self, tab, pattern=None):
		return self.db.hlen(tab)


def connect(*a,**kw):
	return TKVredis(*a,**kw)
