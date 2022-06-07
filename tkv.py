__author__ = 'Maciej Obarski'
__version__ = '0.6.1'
__license__ = 'MIT'

# TODO: per table dumps,loads
# TODO: compression: example, api?
# TODO: redis - patterns
# TODO: mongo - patterns
# TODO: docs
# TODO: patterns working exactly the same on all DB engines
# TODO: table dict interface
# TODO: DB client configuration via single dict and not kwargs?

import itertools
from functools import partial

class TKV:

	# core
	
	def put(self, tab, key, val):           not_implemented(self, 'put')
	def get(self, tab, key, default=None):  not_implemented(self, 'get')
	def has(self, tab, key):                not_implemented(self, 'has')
	def delete(self, tab, key):             not_implemented(self, 'delete')
	def drop(self, tab):                    not_implemented(self, 'drop')
	def size(self, tab):                    not_implemented(self, 'size')
	
	# scanning
	
	def keys(self, tab, pattern=None):      not_implemented(self, 'keys')
	def items(self, tab, pattern=None):     not_implemented(self, 'items')
	def count(self, tab, pattern=None):     not_implemented(self, 'count')
	
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
		not_implemented(self, 'tables')
	
	def flush(self):
		pass
	
	def __del__(self):
		self.flush()
		
	@staticmethod
	def group_keys(keys, pos, sep='/'):
		return itertools.groupby(keys, lambda x:sep.join(x.split(sep)[:pos]))


class KV:
	def __init__(self, tab, tkv):
		methods = [x for x in dir(TKV) if x[0]!='_' and x not in ['tables','group_keys','flush','db','tab']]
		for m in methods:
			setattr(self, m, partial(getattr(tkv, m), tab))

def not_implemented(self, method_name):
	raise NotImplementedError(f'{self.__class__.__name__}.{method_name}')

# ===[ SQLite adapter ]========================================================

import sqlite3
import json

class TKVsqlite(TKV):

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
		except (sqlite3.OperationalError, TypeError):
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
		try:
			return (x[0] for x in self._execute(sql))
		except sqlite3.OperationalError:
			return []
		
		
	def items(self, tab=None, pattern=None):
		if pattern:
			sql = f'select key,val from "{tab}" where key glob "{pattern}"'
		else:
			sql = f'select key,val from "{tab}"'
		try:
			return ((k,self.loads(v)) for k,v in self._execute(sql))
		except sqlite3.OperationalError:
			return []
				
		
	def drop(self, tab):
		self._execute(f'drop table if exists "{tab}"')


	def delete(self, tab, key):
		sql = f'delete from "{tab}" where key=?'
		try:
			self._execute(sql, (key,))
		except sqlite3.OperationalError:
			pass


	def count(self, tab, pattern=None):
		if pattern:
			sql = f'select count(*) from "{tab}" where key glob "{pattern}"'
		else:
			sql = f'select count(*) from "{tab}"'
		try:
			return self._execute(sql).fetchone()[0]
		except sqlite3.OperationalError:
			return 0

	# TODO: from metadata
	def size(self, tab):
		sql = f'select sum(length(val)+length(key)) from "{tab}"'
		try:
			val = self._execute(sql).fetchone()[0] or 0
		except (sqlite3.OperationalError, TypeError):
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


class TKVsqlitetable(TKV):
	
	def __init__(self, path=':memory:', table='tkv', dumps=None, loads=None, **kw):
		self.db = sqlite3.connect(path, **kw)
		self.tab = table
		self.dumps = dumps or json.dumps
		self.loads = loads or json.loads
		self._create()
		
	# core
	
	def put(self, tab, key, val):
		sql = f'replace into "{self.tab}"(tab,key,val) values(?,?,?)'
		val = self.dumps(val)
		self._execute(sql, (tab,key,val))

	def get(self, tab, key, default=None):
		sql = f'select val from "{self.tab}" where tab=? and key=?'
		try:
			val = self._execute(sql, (tab,key)).fetchone()[0]
			val = self.loads(val)
		except TypeError:
			val = default
		return val

	def has(self, tab, key):
		sql = f'select key from "{self.tab}" where tab=? and key=?'
		try:
			return bool(self._execute(sql, (tab,key)).fetchone())
		except sqlite3.OperationalError:
			return False

	def drop(self, tab):
		self._execute(f'delete from "{self.tab}" where tab=?', (tab,))


	def delete(self, tab, key):
		sql = f'delete from "{self.tab}" where tab=? and key=?'
		self._execute(sql, (tab,key))

	def size(self, tab):
		sql = f'select sum(length(val)+length(key)) from "{self.tab}" where tab=?'
		try:
			val = self._execute(sql, (tab,)).fetchone()[0] or 0
		except (sqlite3.OperationalError, TypeError):
			val = 0
		return val

	# scanning

	def keys(self, tab=None, pattern=None):
		if pattern:
			sql = f'select key from "{self.tab}" where tab=? and key glob ?'
			return (x[0] for x in self._execute(sql, (tab,pattern)))
		else:
			sql = f'select key from "{self.tab}" where tab=?'
			return (x[0] for x in self._execute(sql, (tab,)))
		
		
	def items(self, tab=None, pattern=None):
		if pattern:
			sql = f'select key,val from "{self.tab}" where tab=? and key glob ?'
			return ((k,self.loads(v)) for k,v in self._execute(sql, (tab,pattern)))
		else:
			sql = f'select key,val from "{self.tab}" where tab=?'
			return ((k,self.loads(v)) for k,v in self._execute(sql, (tab,)))

	def count(self, tab, pattern=None):
		if pattern:
			sql = f'select count(*) from "{self.tab}" where tab=? and key glob ?'
			return self._execute(sql, (tab,pattern)).fetchone()[0]
		else:
			sql = f'select count(*) from "{self.tab}" where tab=?'
			return self._execute(sql, (tab,)).fetchone()[0]

	# extension
	
	# TODO: get_many
	# TODO: put_items
	
	# other
	
	def tables(self):
		sql = f'select distinct tab from "{self.tab}"'
		return [x[0] for x in self._execute(sql)]

	def flush(self):
		self.db.commit()

	# internals
	
	def _execute(self, sql, *args):
		return self.db.execute(sql, *args)

	def _execute_many(self, sql, *args):
		return self.db.executemany(sql, *args)

	def _create(self):
		sql = f'create table if not exists "{self.tab}" (tab text, key text, val, primary key (tab,key)) without rowid'
		self._execute(sql)


def connect(*a,**kw):
	return TKVsqlite(*a,**kw)

def connect_table(*a,**kw):
	return TKVsqlitetable(*a,**kw)
