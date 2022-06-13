# !!! PRE-ALPHA AS HELL - DO NOT USE IT !!!

# python3 -m pip install --upgrade pip --user
# pip3 install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.6.2/tested_requirements/requirements_36.reqs --user
# pip3 install snowflake-connector-python==2.6.2 --user
# https://docs.snowflake.com/en/user-guide/python-connector-api.html#label-python-connector-data-type-mappings

"""
https://community.snowflake.com/s/question/0D50Z00008BDIPTSA5/snowflake-oltp
> I am delighted to report that with the introduction of Hybrid Tables
> (to be formally announced at our Snowflake Summit in a couple of weeks), # 2022-06-03
> Snowflake will be able to handle OLTP workloads.
"""

import snowflake.connector
import tkv

# TODO: multicolumn keys
class VTKVsnowflake(tkv.VTKV):
	
	def __init__(self, sch, **kw):
		self.db = snowflake.connector.connect(**kw)
		self.sch = sch
		self.sep_tab = '/'
		self.sep_col = ','
		self.cte = ''
		self.dumps = None # NOT USED - required for self.table
		self.loads = None # NOT USED - required for self.table
	
	# core - read
	
	def get(self, tab, key, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{self.sch}"."{db_tab}" where "{db_key}"=%s'
		sql = self._get_sql_with_cte(sql, tab)
		return self._execute(sql, (key,)).fetchone()[0] # TODO

	def has(self, tab, key):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}" from "{self.sch}"."{db_tab}" where "{db_key}"=%s'
		sql = self._get_sql_with_cte(sql, tab)
		return bool(self._execute(sql, (key,)).fetchone())

	def size(self, tab):
		not_implemented(self, 'size')

	# iterators
	
	def keys(self, tab, sort=False):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_col}" from "{self.sch}"."{db_tab}" order by "{db_key}"'
		sql = self._get_sql_with_cte(sql, tab)
		return self._execute(sql)
	
	def items(self, tab, sort=False):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select "{db_key}","{db_col}" from "{self.sch}"."{db_tab}" order by "{db_key}"'
		sql = self._get_sql_with_cte(sql, tab)
		return ((x[0],x[1:]) for x in self._execute(sql))
	

	# extension
	
	def get_many(self, tab, keys, default=None):
		db_tab, db_key, db_col = self._parse_tab(tab)
		placeholders = self.sep_col.join(['%s']*len(keys))
		sql = f'select "{db_key}","{db_col}" from "{self.sch}"."{db_tab}" where "{db_key}" in ({placeholders})'
		sql = self._get_sql_with_cte(sql, tab)
		if self.sep_col in db_col: # support for multicolumn values
			resp = {x[0]:x[1:] for x in self._execute(sql,keys)}
		else:
			resp = dict(self._execute(sql,keys))
		return [resp[k] if k in resp else default for k in keys]
	
	def count(self, tab):
		db_tab, db_key, db_col = self._parse_tab(tab)
		sql = f'select count(*) from "{self.sch}"."{db_tab}"'
		sql = self._get_sql_with_cte(sql, tab)
		return self._execute(sql).fetchone()[0] # TODO

	# sugar

	# internal
	
	def _execute(self, sql, *a):
		print('SQL>',sql) # xxx
		cur = self.db.cursor()
		results = cur.execute(sql,*a),0
		# columns = [x.name.lower() for x in cur.description]
		return results

	def table(self, table, sql=''):
		tab = tkv.TKV.table(self, table)
		tab.tkv.cte = sql
		return tab


def connect_view(**kw):
	db = VTKVsnowflake(**kw)
