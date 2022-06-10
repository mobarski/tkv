import tkv
import sqlite3
import json


class TKVsqlitetable(tkv.TKV):
	
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

	# iterators

	def keys(self, tab, sort=False):
		sql = f'select key from "{self.tab}" where tab=?'
		return (x[0] for x in self._execute(sql, (tab,)))
		
		
	def items(self, tab, sort=False):
		sql = f'select key,val from "{self.tab}" where tab=?'
		return ((k,self.loads(v)) for k,v in self._execute(sql, (tab,)))

	def count(self, tab, sort=False):
		sql = f'select count(*) from "{self.tab}" where tab=?'
		return self._execute(sql, (tab,)).fetchone()[0]

	# scanning

	def scan_keys(self, tab, pattern, sort=False):
		sql = f'select key from "{self.tab}" where tab=? and key glob ?'
		return (x[0] for x in self._execute(sql, (tab,pattern)))
			
	def scan_items(self, tab, pattern, sort=False):
		sql = f'select key,val from "{self.tab}" where tab=? and key glob ?'
		return ((k,self.loads(v)) for k,v in self._execute(sql, (tab,pattern)))

	def scan_count(self, tab, pattern, sort=False):
		sql = f'select count(*) from "{self.tab}" where tab=? and key glob ?'
		return self._execute(sql, (tab,pattern)).fetchone()[0]

	# extension
	

	def get_many(self, tab, keys, default=None):
		placeholders = ','.join('?'*len(keys))
		sql = f'select key,val from "{self.tab}" where tab="{tab}" and key in ({placeholders})'
		try:
			resp = dict(self._execute(sql, keys))
		except sqlite3.OperationalError:
			resp = {}
		return [self.loads(resp[k]) if k in resp else default for k in keys]

	def put_items(self, tab, items):
		sql = f'replace into "{self.tab}"(tab,key,val) values(?,?,?)'
		items = [(tab, k, self.dumps(v)) for k,v in items]
		try:
			self._execute_many(sql, items)
		except sqlite3.OperationalError:
			self._create(tab)
			self._execute_many(sql, items)
	
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



class TKVsqliteview(tkv.VTKV):
	
	def __init__(self, path, **kw):
		self.db = sqlite3.connect(path, **kw)
		self.sep_tab = '/'
		self.sep_col = ','
		self.dumps = None # NOT USED - required for self.table
		self.loads = None # NOT USED - required for self.table
	
	def get(self, tab, key, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{db_tab}" where "{db_key}"=?'
		return self._execute(sql, (key,)).fetchone()[0]

	def has(self, tab, key):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}" from "{db_tab}" where "{db_key}"=?'
		return bool(self._execute(sql, (key,)).fetchone())

	def size(self, tab):
		not_implemented(self, 'size')

	# iterators
	
	def keys(self, tab, sort=False):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{db_tab}" order by "{db_key}"'
		return self._execute(sql)
	
	def items(self, tab, sort=False):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}","{db_col}" from "{db_tab}" order by "{db_key}"'
		return ((x[0],x[1:]) for x in self._execute(sql))

	# extension
	
	def get_many(self, tab, keys, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		placeholders = self.sep_col.join(['?']*len(keys))
		sql = f'select "{db_key}","{db_col}" from "{db_tab}" where "{db_key}" in ({placeholders})'
		if self.sep_col in db_col: # support for multicolumn values
			resp = {x[0]:x[1:] for x in self._execute(sql,keys)}
		else:
			resp = dict(self._execute(sql,keys))
		return [resp[k] if k in resp else default for k in keys]
	
	def count(self, tab):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select count(*) from "{db_tab}"'
		return self._execute(sql).fetchone()[0]

	# internal
	
	def _parse_tab(self, tab):
		tab,key,col = tab.upper().split(self.sep_tab)[:3]
		if self.sep_col in col: # support for multicolumn values
			col = col.replace(self.sep_col,f'"{self.sep_col}"')
		return tab,key,col
		
	def _execute(self, sql, *a):
		print('SQL>',sql) # xxx
		results = self.db.execute(sql,*a)
		# columns = [x.name.lower() for x in cur.description]
		return results


class TKVsqlitedb(tkv.TKV):

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

	def keys(self, tab, sort=False):
		sql = f'select key from "{tab}"'
		try:
			return (x[0] for x in self._execute(sql))
		except sqlite3.OperationalError:
			return []
		
		
	def items(self, tab, sort=False):
		sql = f'select key,val from "{tab}"'
		try:
			return ((k,self.loads(v)) for k,v in self._execute(sql))
		except sqlite3.OperationalError:
			return []

	def scan_keys(self, tab, pattern, sort=False):
		sql = f'select key from "{tab}" where key glob "{pattern}"'
		try:
			return (x[0] for x in self._execute(sql))
		except sqlite3.OperationalError:
			return []
		
		
	def scan_items(self, tab, pattern, sort=False):
		sql = f'select key,val from "{tab}" where key glob "{pattern}"'
		try:
			return ((k,self.loads(v)) for k,v in self._execute(sql))
		except sqlite3.OperationalError:
			return []

	def scan_count(self, tab, pattern):
		sql = f'select count(*) from "{tab}" where key glob "{pattern}"'
		try:
			return self._execute(sql).fetchone()[0]
		except sqlite3.OperationalError:
			return 0
			
		
	def drop(self, tab):
		self._execute(f'drop table if exists "{tab}"')


	def delete(self, tab, key):
		sql = f'delete from "{tab}" where key=?'
		try:
			self._execute(sql, (key,))
		except sqlite3.OperationalError:
			pass


	def count(self, tab):
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



def connect(*a,**kw):
	return TKVsqlitetable(*a,**kw)

def connect_alt(*a,**kw):
	return TKVsqlitedb(*a,**kw)

def connect_view(*a,**kw):
	return TKVsqliteview(*a,**kw)
