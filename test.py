import doctest

def test_engine(e, verbose=False):
	if e == 'sqlite':
		import tkv_sqlite
		db = tkv_sqlite.connect()

	elif e == 'sqlite_alt':
		import tkv_sqlite
		db = tkv_sqlite.connect_alt()

	elif e == 'sqlite_view':
		import tkv_sqlite
		db = tkv_sqlite.connect_view()
	
	elif e == 'duckdb':
		import tkv_duckdb
		db = tkv_duckdb.connect()

	elif e == 'duckdb_view':
		import tkv_duckdb
		db = tkv_duckdb.connect_view()
	
	elif e == 'redis':
		import tkv_redis
		db = tkv_redis.connect(host='127.0.0.1', port=6379)
		
	elif e == 'redis_ssdb':
		import tkv_redis
		db = tkv_redis.connect(host='127.0.0.1', port=8888)
		
	elif e == 'redis_keydb':
		import tkv_redis
		db = tkv_redis.connect(host='127.0.0.1', port=6379) # TODO
		
	elif e == 'snowflake':
		import tkv_snowflake
		db = tkv_snowflake.connect() # TODO
		
	elif e == 'mongo':
		import tkv_mongo
		db = tkv_mongo.connect() # TODO
		
	else:
		raise Exception(f'Unknown engine: {e}')
	#
	suite = 'test_view.txt' if 'view' in e else 'test.txt'
	doctest.testfile(suite, name=e, globs={'db':db}, verbose=verbose)

if __name__=="__main__":
	import sys
	engines = sys.argv[1:]
	if not engines:
		print('USAGE: test.py engine1 engine2 ...')
	
	for e in engines:
		print(f'testing: {e}')
		test_engine(e, verbose=False)
