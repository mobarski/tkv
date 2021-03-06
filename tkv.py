__author__ = 'Maciej Obarski'
__version__ = '0.6.7'
__license__ = 'MIT'

import itertools
import sys
from functools import partial

class TKV:

	# core - read
	
	def get(self, tab, key, default=None):
		not_implemented(self, 'get')
		
	def has(self, tab, key):
		not_implemented(self, 'has')
	
	# core - write
	
	def put(self, tab, key, val):
		not_implemented(self, 'put')
		
	def drop(self, tab):
		not_implemented(self, 'drop')
		
	def delete(self, tab, key):
		not_implemented(self, 'delete')
	
	# iterators
	
	def keys(self, tab, sort=False, limit=None):
		not_implemented(self, 'keys')
	
	def items(self, tab, sort=False, limit=None):
		return (self.get(tab, k) for k in self.keys(tab, sort, limit))
	
	def values(self, tab, sort=False, limit=None):
		return (v for k,v in self.items(tab, sort, limit))
	
	# scan
	
	def scan_keys(self, tab, pattern, sort=False, limit=None):
		not_implemented(self, 'scan_keys')
	
	def scan_items(self, tab, pattern, sort=False, limit=None):
		return ((k,self.get(tab,k)) for k in self.scan_keys(tab, pattern, sort, limit))
	
	def scan_values(self, tab, pattern, sort=False, limit=None):
		return (v for k,v in self.scan_items(tab, pattern, sort, limit))
		
	def scan_count(self, tab, pattern):
		return iter_len(self.scan_keys(tab, pattern))
	
	# other - read

	def size(self, tab):
		not_implemented(self, 'size')
		
	def tables(self):
		not_implemented(self, 'tables')
	
	def get_many(self, tab, keys, default=None):
		return [self.get(tab, key, default) for key in keys]
				
	def count(self, tab):
		return iter_len(self.keys(tab))
			
	def table(self, tab, dumps=None, loads=None):
		return KV(tab, self, dumps, loads)
	
	# other - write

	def put_items(self, tab, items):
		for k,v in items:
			self.put(tab, k, v)

	def flush(self):
		pass # might not be needed in some engines

	# sugar

	def get_items(self, tab, keys, default=None):
		return list(zip(keys, self.get_many(tab, keys, default)))

	def put_many(self, tab, keys, values):
		self.put_items(tab, zip(keys, values))
		
	# ...
	
	def __del__(self):
		self.flush()
	
	def _sql_limit(self, limit):
		return f'limit {limit}' if limit is not None else ''
	
	@staticmethod
	def group_keys(keys, pos, sep='/'):
		return itertools.groupby(keys, lambda x:sep.join(x.split(sep)[:pos]))


from copy import copy
class KV:
	def __init__(self, tab, tkv, dumps, loads):
		self.dumps = dumps or tkv.dumps
		self.loads = loads or tkv.loads
		self.tab = tab
		self.tkv = copy(tkv)
		self.tkv.dumps = self.dumps
		self.tkv.loads = self.loads
		methods = [x for x in dir(TKV) if x[0]!='_' and x not in ['tables','group_keys','flush','db','tab']]
		for m in methods:
			setattr(self, m, partial(getattr(self.tkv, m), tab))

# WIP
class VTKV(TKV):
	"""Virtual TKV - read-only view on existing relational db schema.
	KV interface can access various columns based on information in the table name ie:
	> get('users/uid/name', 42)   # select name    from users  where uid=42
	> get('cities/id/lat,lon', 1) # select lat,lon from cities where id=1
	"""

	placeholder = '?'
	#
	sep_tab = '/'
	sep_col = ','
	cte = ''
	dumps = None # NOT USED - required for self.table
	loads = None # NOT USED - required for self.table
	echo = False
	# for stage_items
	types = {int:'bigint', float:'double', str:'text'}
	create_table_sql_suffix = ''

	# core

	def get(self, tab, key, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{db_tab}" where "{db_key}"={self.placeholder}'
		sql = self._get_sql_with_cte(sql, tab)
		if self.sep_col in db_col:
			return self._execute_fetchone(sql, (key,))
		else:
			return self._execute_fetchone(sql, (key,))[0]

	def has(self, tab, key):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}" from "{db_tab}" where "{db_key}"={self.placeholder}'
		sql = self._get_sql_with_cte(sql, tab)
		return bool(self._execute_fetchone(sql, (key,)))

	def size(self, tab):
		not_implemented(self, 'size')

	# iterators
	
	def keys(self, tab, sort=False, limit=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		lim = self._sql_limit(limit)
		sql = f'select "{db_col}" from "{db_tab}" order by "{db_key}" {lim}'
		sql = self._get_sql_with_cte(sql, tab)
		return (x[0] for x in self._execute(sql))
	
	def items(self, tab, sort=False, limit=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		lim = self._sql_limit(limit)
		sql = f'select "{db_key}","{db_col}" from "{db_tab}" order by "{db_key}" {lim}'
		sql = self._get_sql_with_cte(sql, tab)
		if self.sep_col in db_col:
			return ((x[0],x[1:]) for x in self._execute(sql))
		else:
			return ((x[0],x[1]) for x in self._execute(sql))

	def values(self, tab, sort=False, limit=None):
		return (v for k,v in self.items(tab, sort, limit))

	# extension
	
	def get_many(self, tab, keys, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		placeholders = self._get_placeholders(len(keys))
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
		return self._execute_fetchone(sql)[0]

	# internal

	def table(self, table, sql=''):
		tab = TKV.table(self, table)
		tab.tkv.cte = sql
		return tab

	def _execute_fetchone(self, sql, *a):
		self._echo(sql)
		results = self.db.execute(sql,*a).fetchone()
		return results

	def _execute(self, sql, *a):
		self._echo(sql)
		results = self.db.execute(sql,*a)
		return results

	def _execute_many(self, sql, *args):
		self._echo(sql)
		results = self.db.executemany(sql, *args)
		return results
	
	def _parse_tab(self, tab):
		tab,key,col = tab.upper().split(self.sep_tab)[:3]
		if self.sep_col in col: # support for multicolumn values
			col = col.replace(self.sep_col,f'"{self.sep_col}"')
		return tab,key,col

	def _get_sql_with_cte(self, sql, tab):
		db_tab, db_key, db_col = self._parse_tab(tab)
		before = f'with "{db_tab}" as ({self.cte})' if self.cte else ''
		return before + ' ' + sql
	
	def _get_placeholders(self, n):
		return ','.join([self.placeholder]*n)

	def _echo(self, sql):
		if self.echo:
			print('SQL>', sql, file=sys.stderr)
			sys.stderr.flush()

	# experimental
	
	def stage_items(self, table, items):
		if not items: return
		tab,key,col = table.upper().split(self.sep_tab)[:3]
		cols = col.split(self.sep_col)
		#
		types = [self.types.get(type(x),'') for x in items[0][1]]
		key_type = self.types.get(type(items[0][0]))
		#
		columns = ','.join([f'"{c}" {t}' for c,t in zip(cols,types)])
		sql = f'drop table if exists "{tab}"'
		self._execute(sql)
		sep = f'"{self.sep_col}"'
		sql = f'create table "{tab}"("{key}" {key_type} primary key, {columns}) {self.create_table_sql_suffix}'
		self._execute(sql)
		#
		placeholders = self._get_placeholders(len(cols)+1)
		sql = f'insert into "{tab}" values({placeholders})'
		flat_items = [(x[0],*x[1]) for x in items] if len(cols)>1 else items
		self._execute_many(sql, flat_items)

def iter_len(iterable):
	cnt = 0
	for _ in iterable:
		cnt += 1
	return cnt

def not_implemented(self, method_name):
	raise NotImplementedError(f'{self.__class__.__name__}.{method_name}')
