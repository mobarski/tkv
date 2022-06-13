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
	
	def __init__(self, path=':memory:', **kw):
		self.db = sqlite3.connect(path, **kw)
		self.sep_tab = '/'
		self.sep_col = ','
		self.cte = ''
		self.dumps = None # NOT USED - required for self.table
		self.loads = None # NOT USED - required for self.table
	
	def _get_sql_with_cte(self, sql, tab):
		db_tab, db_key, db_col = self._parse_tab(tab)
		before = f'with "{db_tab}" as ({self.cte})' if self.cte else ''
		return before + ' ' + sql
	
	def get(self, tab, key, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{db_tab}" where "{db_key}"=?'
		sql = self._get_sql_with_cte(sql, tab)
		if self.sep_col in db_col:
			return self._execute(sql, (key,)).fetchone()
		else:
			return self._execute(sql, (key,)).fetchone()[0]

	def has(self, tab, key):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}" from "{db_tab}" where "{db_key}"=?'
		sql = self._get_sql_with_cte(sql, tab)
		return bool(self._execute(sql, (key,)).fetchone())

	def size(self, tab):
		not_implemented(self, 'size')

	# iterators
	
	def keys(self, tab, sort=False):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{db_tab}" order by "{db_key}"'
		sql = self._get_sql_with_cte(sql, tab)
		return (x[0] for x in self._execute(sql))
	
	def items(self, tab, sort=False):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}","{db_col}" from "{db_tab}" order by "{db_key}"'
		sql = self._get_sql_with_cte(sql, tab)
		if self.sep_col in db_col:
			return ((x[0],x[1:]) for x in self._execute(sql))
		else:
			return ((x[0],x[1]) for x in self._execute(sql))

	# extension
	
	def get_many(self, tab, keys, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		placeholders = self.sep_col.join(['?']*len(keys))
		sql = f'select "{db_key}","{db_col}" from "{db_tab}" where "{db_key}" in ({placeholders})'
		sql = self._get_sql_with_cte(sql, tab)
		if self.sep_col in db_col: # support for multicolumn values
			resp = {x[0]:x[1:] for x in self._execute(sql,keys)}
		else:
			resp = dict(self._execute(sql,keys))
		return [resp[k] if k in resp else default for k in keys]
	
	def count(self, tab):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select count(*) from "{db_tab}"'
		sql = self._get_sql_with_cte(sql, tab)
		return self._execute(sql).fetchone()[0]

	# internal
			
	def _execute(self, sql, *a):
		#print('SQL>',sql) # xxx
		results = self.db.execute(sql,*a)
		# columns = [x.name.lower() for x in cur.description]
		return results

	def _execute_many(self, sql, *args):
		#print('SQL>',sql)
		results = self.db.executemany(sql, *args)
		return results
	
	def table(self, table, sql=''):
		tab = tkv.TKV.table(self, table)
		tab.tkv.cte = sql
		return tab

	# experimental
	
	def stage_items(self, table, items):
		tab,key,col = table.upper().split(self.sep_tab)[:3]
		cols = col.split(self.sep_col)
		sql = f'drop table if exists "{tab}"'
		self._execute(sql)
		sep = f'"{self.sep_col}"'
		sql = f'create table "{tab}"("{key}" primary key,"{sep.join(cols)}") without rowid'
		self._execute(sql)
		placeholders = ','.join(['?']*(len(cols)+1))
		sql = f'insert into "{tab}" values({placeholders})'
		flat_items = [(x[0],*x[1]) for x in items] if len(cols)>1 else items
		self._execute_many(sql, flat_items)


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

if __name__=="__main__":
	db = connect_view(':memory:')
	db.db.execute('create table abc (a,b,c)')
	db.db.execute('insert into abc values (1,11,111)')
	db.db.execute('insert into abc values (2,22,222)')
	db.db.execute('insert into abc values (3,33,333)')
	db.db.execute('insert into abc values (4,44,444)')
	tab = db.table('xxx/k/v', sql='select a as k,b+c as v from abc')
	#tab = db.table('abc/a/b')
	print(tab.get(1))
	print(list(tab.keys()))
	print(list(tab.items()))
	print(list(tab.values()))
	db.stage_items('aaa/k/a,b,c', [(1,(11,22,33)),(2,(22,33,44))])
	print(list(db.db.execute('select * from aaa')))
	db.stage_items('bbb/k/a', [(1,11),(2,22)])
	print(list(db.db.execute('select * from bbb')))
	