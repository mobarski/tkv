# !!! PRE-ALPHA AS HELL - DO NOT USE IT !!!

import tkv
import duckdb
import json

class TKVduckdbtable(tkv.TKV):
	
	def __init__(self, path=':memory:', table='tkv', dumps=None, loads=None, **kw):
		self.db = duckdb.connect(path, **kw)
		self.tab = table
		self.dumps = dumps or json.dumps
		self.loads = loads or json.loads
		self._create()
		
	# core
	
	def put(self, tab, key, val):
		sql = f'delete from "{self.tab}" where tab=? and key=?'
		self._execute(sql, (tab,key))
		sql = f'insert into "{self.tab}"(tab,key,val) values(?,?,?)'
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
		except sqlite3.OperationalError: # TODO
			return False

	def drop(self, tab):
		self._execute(f'delete from "{self.tab}" where tab=?', (tab,))


	def delete(self, tab, key):
		sql = f'delete from "{self.tab}" where tab=? and key=?'
		self._execute(sql, (tab,key))

	def size(self, tab):
		#sql = f'select sum(octet_length(val)+length(key)) from "{self.tab}" where tab=?'
		sql = f'select sum(length(val)+length(key)) from "{self.tab}" where tab=?'
		try:
			val = self._execute(sql, (tab,)).fetchone()[0] or 0
		except (sqlite3.OperationalError, TypeError):
			val = 0
		return val

	# iterators

	def keys(self, tab, sort=False):
		sql = f'select key from "{self.tab}" where tab=? order by key'
		return (x[0] for x in self._execute(sql, (tab,)).fetchall())
		
		
	def items(self, tab, sort=False):
		sql = f'select key,val from "{self.tab}" where tab=? order by key'
		return ((k,self.loads(v)) for k,v in self._execute(sql, (tab,)).fetchall())

	def count(self, tab, sort=False):
		sql = f'select count(*) from "{self.tab}" where tab=?'
		return self._execute(sql, (tab,)).fetchone()[0]

	# scanning

	def scan_keys(self, tab, pattern, sort=False):
		sql = f'select key from "{self.tab}" where tab=? and key glob ? order by key'
		return (x[0] for x in self._execute(sql, (tab,pattern)).fetchall())
			
	def scan_items(self, tab, pattern, sort=False):
		sql = f'select key,val from "{self.tab}" where tab=? and key glob ? order by key'
		return ((k,self.loads(v)) for k,v in self._execute(sql, (tab,pattern)).fetchall())

	def scan_count(self, tab, pattern, sort=False):
		sql = f'select count(*) from "{self.tab}" where tab=? and key glob ?'
		return self._execute(sql, (tab,pattern)).fetchone()[0]

	# extension
	

	def get_many(self, tab, keys, default=None):
		placeholders = ','.join('?'*len(keys))
		sql = f'''select key,val from "{self.tab}" where tab='{tab}' and key in ({placeholders})'''
		try:
			resp = dict(self._execute(sql, keys).fetchall())
		except sqlite3.OperationalError:
			resp = {}
		return [self.loads(resp[k]) if k in resp else default for k in keys]

	# other
	
	def tables(self):
		sql = f'select distinct tab from "{self.tab}"'
		return [x[0] for x in self._execute(sql).fetchall()]

	def flush(self):
		self.db.commit()

	# internals
	
	def _execute(self, sql, *args):
		return self.db.execute(sql, *args)

	def _execute_many(self, sql, *args):
		return self.db.executemany(sql, *args)

	def _create(self):
		sql = f'create table if not exists "{self.tab}" (tab text, key text, val text, primary key (tab,key))'
		self._execute(sql)


class TKVduckdbview(tkv.VTKV):
	
	def __init__(self, path=':memory:', **kw):
		self.db = duckdb.connect(path, **kw)

	# internal

	def _execute(self, sql, *a):
		self._echo(sql)
		results = self.db.execute(sql,*a).fetchall()
		return results

def connect(*a,**kw):
	return TKVduckdbtable(*a,**kw)

def connect_view(*a,**kw):
	return TKVduckdbview(*a,**kw)

