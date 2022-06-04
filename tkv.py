__author__ = 'Maciej Obarski'
__version__ = '0.2.1'
__license__ = 'MIT'

# TODO: docs
# TODO: KVtable
# TODO: patterns working exactly the same on all DB engines
# TODO: benchmark
# TODO: DB client configuration via single dict and not kwargs?
# TODO: single table mode (separate class)?
# TODO: iterators vs lists

class TKV:

	# core
	def put(self, tab, key, val):
		pass
	def get(self, tab, key, default=None):
		pass
	def has(self, tab, key):
		pass
	def keys(self, tab, pattern=None):
		pass
	def count(self, tab, pattern=None):
		pass
	def items(self, tab, pattern=None):
		pass	
	def delete(self, tab, key):
		pass
	def drop(self, tab):
		pass
	def tables(self, pattern=None):
		pass
		
	# extension (can be reimplemented in the child for better performance)
	
	def get_many(self, tab, keys, default=None):
		return [self.get(tab, key, default) for key in keys]
		
	def put_many(self, tab, keys, values):
		for k,v in zip(keys, values):
			self.put(tab, k, v)
		
	# sugar
	
	def put_dict(self, tab, dict):
		items = dict.items()
		self.put_many(tab, [x[0] for x in items], [x[1] for x in items])
		
	def get_dict(self, tab, keys, default=None):
		return dict(zip(keys, self.get_many(tab, keys, default)))
		
	def values(self, tab, pattern=None):
		return (x[1] for x in self.items(tab, pattern))

	# table
	
	def table(self, tab):
		return KV(tab, self)


from functools import partial
class KV:
	def __init__(self, tab, tkv):
		methods = [x for x in dir(TKV) if x[0]!='_' and x not in ['tables']]
		for m in methods:
			setattr(self, m, partial(getattr(tkv, m), tab))

# ===[ SQLite adapter ]========================================================

import sqlite3
import json

class TKVlite(TKV):

	def __init__(self, path=':memory:', dumps=None, loads=None):
		self.db = sqlite3.connect(path)
		self.dumps = dumps or json.dumps
		self.loads = loads or json.loads

	# core
	
	def put(self, tab, key, val):
		sql = f'replace into "{tab}"(key,val) values(?,?)'
		val = self.dumps(val)
		try:
			self._execute(sql, (key,val))
		except sqlite3.OperationalError:
			self._create(tab)
			self._execute(sql, (key,val))


	def get(self, tab, key, default=None):
		sql = f'select val from "{tab}" where key=?'
		try:
			val = self._execute(sql, (key,)).fetchone()[0]
			val = self.loads(val)
		except: # TODO
			val = default
		return val


	def has(self, tab, key):
		sql = f'select key from "{tab}" where key=?'
		try:
			return bool(self._execute(sql, (key,)).fetchone())
		except sqlite3.OperationalError:
			return False

	def keys(self, tab=None, pattern=None):
		if pattern:
			sql = f'select key from "{tab}" where key glob "{pattern}"'
		else:
			sql = f'select key from "{tab}"'
		return (x[0] for x in self._execute(sql))
		
		
	def items(self, tab=None, pattern=None):
		if pattern:
			sql = f'select key,val from "{tab}" where key glob "{pattern}"'
		else:
			sql = f'select key,val from "{tab}"'
		return ((k,self.loads(v)) for k,v in self._execute(sql))
		
		
	def tables(self):
		sql = 'select name from sqlite_master where type="table"'
		return [x[0] for x in self._execute(sql)]
		
		
	def drop(self, tab):
		self._execute(f'drop table if exists {tab}')
		self._delete('',tab,tab='meta')


	def delete(self, tab, key):
		sql = f'delete from "{tab}" where key=?'
		self._execute(sql, (key,))


	def count(self, tab=None, pattern=None):
		if pattern:
			sql = f'select count(*) from "{tab}" where key glob "{pattern}"'
		else:
			sql = f'select count(*) from "{tab}"'
		return self._execute(sql).fetchone()[0]

	# extension

	# TODO: optimized get_many
	# TODO: optimized put_many

	# internals
	
	def _execute(self, sql, *args):
		return self.db.execute(sql, *args)
	
	def _create(self, tab):
		sql = f'create table if not exists "{tab}" (key text primary key, val) without rowid'
		self._execute(sql)


def connect(*a,**kw):
	return TKVlite(*a,**kw)

