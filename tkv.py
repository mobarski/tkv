__author__ = 'Maciej Obarski'
__version__ = '0.4.1'
__license__ = 'MIT'

# TODO: SQL injection prevention !!!

# TODO: single table mode (separate class)
# TODO: benchmark
# TODO: compression
# TODO: patterns working exactly the same on all DB engines
# TODO: per table compression,dumps,loads
# TODO: docs
# TODO: tkv_mongo
# TODO: tkv_snowflake
# TODO: DB client configuration via single dict and not kwargs?

import itertools
from functools import partial

class TKV:

	# core
	
	def put(self, tab, key, val):
		pass
	def get(self, tab, key, default=None):
		pass
	def has(self, tab, key):
		pass
	def delete(self, tab, key):
		pass
	def drop(self, tab):
		pass
	def size(self, tab):
		pass
	
	# scanning
	
	def keys(self, tab, pattern=None):
		pass
	def items(self, tab, pattern=None):
		pass
	def count(self, tab, pattern=None):
		pass
		
	# extension (can be reimplemented in the child for better performance)
	
	def get_many(self, tab, keys, default=None):
		return [self.get(tab, key, default) for key in keys]
				
	def put_items(self, tab, items):
		for k,v in items:
			self.put(tab, k, v)

	# sugar

	def get_items(self, tab, keys, default=None):
		return list(zip(keys, self.get_many(tab, keys, default)))

	def put_many(self, tab, keys, values):
		self.put_items(tab, zip(keys, values))
		
	def values(self, tab, pattern=None):
		return (v for k,v in self.items(tab, pattern))

	# other

	def table(self, tab):
		return KV(tab, self)

	def tables(self):
		pass
	
	def flush(self):
		pass
	
	def __del__(self):
		self.flush()
	
	@staticmethod
	def group_keys(keys, pos, sep=':'):
		return itertools.groupby(keys, lambda x:sep.join(x.split(sep)[:pos]))


class KV:
	def __init__(self, tab, tkv):
		methods = [x for x in dir(TKV) if x[0]!='_' and x not in ['tables','group_keys','flush']]
		for m in methods:
			setattr(self, m, partial(getattr(tkv, m), tab))


# ===[ SQLite adapter ]========================================================

import sqlite3
import json

class TKVlite(TKV):

	def __init__(self, path=':memory:', dumps=None, loads=None, **kw):
		self.db = sqlite3.connect(path, **kw)
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
		except sqlite3.OperationalError:
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
				
		
	def drop(self, tab):
		self._execute(f'drop table if exists {tab}')


	def delete(self, tab, key):
		sql = f'delete from "{tab}" where key=?'
		self._execute(sql, (key,))


	def count(self, tab, pattern=None):
		if pattern:
			sql = f'select count(*) from "{tab}" where key glob "{pattern}"'
		else:
			sql = f'select count(*) from "{tab}"'
		return self._execute(sql).fetchone()[0]

	# TODO: from metadata
	def size(self, tab):
		sql = f'select sum(length(val)+length(key)) from "{tab}"'
		try:
			val = self._execute(sql).fetchone()[0] or 0
		except sqlite3.OperationalError:
			val = 0
		return val

	# extension

	def get_many(self, tab, keys, default=None):
		placeholders = ','.join('?'*len(keys))
		sql = f'select key,val from "{tab}" where key in ({placeholders})'
		try:
			resp = dict(self._execute(sql,keys))
		except sqlite3.OperationalError:
			resp = {}
		return [self.loads(resp[k]) if k in resp else default for k in keys]

	def put_items(self, tab, items):
		sql = f'replace into "{tab}"(key,val) values(?,?)'
		items = [(k,self.dumps(v)) for k,v in items]
		try:
			self._execute_many(sql, items)
		except sqlite3.OperationalError:
			self._create(tab)
			self._execute_many(sql, items)

	# other

	def tables(self):
		sql = 'select name from sqlite_master where type="table"'
		return [x[0] for x in self._execute(sql)]

	def flush(self):
		self.db.commit()

	# internals
	
	def _execute(self, sql, *args):
		return self.db.execute(sql, *args)

	def _execute_many(self, sql, *args):
		return self.db.executemany(sql, *args)

	
	def _create(self, tab):
		sql = f'create table if not exists "{tab}" (key text primary key, val) without rowid'
		self._execute(sql)


def connect(*a,**kw):
	return TKVlite(*a,**kw)

