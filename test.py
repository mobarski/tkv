import doctest

def test_engine(e, verbose=False):
	if e == 'sqlite':
		import tkv_sqlite
		db = tkv_sqlite.connect()

	elif e == 'sqlite_table':
		import tkv_sqlite
		db = tkv_sqlite.connect_table()
	
	elif e == 'duckdb':
		import tkv_duckdb
		db = tkv_duckdb.connect()
	
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
	doctest.testfile('test.txt', name=e, globs={'db':db}, verbose=verbose)

if __name__=="__main__":
	import sys
	# TODO: engines from sys.argv
	# TODO: verbose from sys.argv
	engines = ['duckdb']
	
	for e in engines:
		test_engine(e, verbose=False)
