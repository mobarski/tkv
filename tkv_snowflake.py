# !!! PRE-ALPHA AS HELL - DO NOT USE IT !!!

# python3 -m pip install --upgrade pip --user
# pip3 install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.6.2/tested_requirements/requirements_36.reqs --user
# pip3 install snowflake-connector-python==2.6.2 --user
# https://docs.snowflake.com/en/user-guide/python-connector-api.html#label-python-connector-data-type-mappings

"""
https://community.snowflake.com/s/question/0D50Z00008BDIPTSA5/snowflake-oltp
> I am delighted to report that with the introduction of Hybrid Tables
> (to be formally announced at our Snowflake Summit in a couple of weeks), # 2022-06-03 -> 2022-06-19
> Snowflake will be able to handle OLTP workloads.
"""

import snowflake.connector
import tkv

class VTKVsnowflake(tkv.VTKV):
	placeholder = '%s'
	
	def __init__(self, db=None, sch=None, role=None, **kw):
		self.db = snowflake.connector.connect(**kw).cursor()
		if role:
			self._execute(f'use role {role}')
		if db:
			self._execute(f'use database {db}')
		if sch:
			self._execute(f'use schema {sch}')


def connect_view(**kw):
	return VTKVsnowflake(**kw)

