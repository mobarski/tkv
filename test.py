
# yep... for now this is primitive as hell

def test1(db):

	db.put('a','x',1)
	db.put('a','y',2)
	db.put('a','z',3)

	db.put('b','r',11)
	db.put('b','s',22)

	assert db.get('a','x') == 1
	assert db.get('a','y') == 2
	assert db.get('a','z') == 3
	assert db.get('b','r') == 11
	assert db.get('b','s') == 22
	assert db.get('a','v',-1) == -1
	assert db.get('c','v',-1) == -1
	
	assert db.has('a','x') == True
	assert db.has('a','v') == False
	assert db.has('c','v') == False
	
	assert db.count('a') == 3
	assert db.count('b') == 2
	
	assert db.get_many('a',['x','y','z']) == [1,2,3]
	assert db.get_many('b',['r','s','t'],-1) == [11,22,-1]
	
	assert set(db.tables()) == set('ab')
	
	assert set(db.keys('a')) == set('xyz')
	assert set(db.keys('b')) == set('rs')

def test2(db):
	t1 = db.table('a')
	t2 = db.table('b')
	
	t1.put('x',1)
	t1.put('y',2)
	t1.put('z',3)

	t2.put('r',11)
	t2.put('s',22)

	assert t1.get('x') == 1
	assert t1.get('y') == 2
	assert t1.get('z') == 3
	assert t1.get('v',-1) == -1
	assert t2.get('r') == 11
	assert t2.get('s') == 22
	
	assert t1.has('x') == True
	assert t1.has('v') == False
	
	assert t1.count() == 3
	assert t2.count() == 2
	
	assert t1.get_many(['x','y','z']) == [1,2,3]
	assert t2.get_many(['r','s','t'],-1) == [11,22,-1]
	
	assert set(t1.keys()) == set('xyz')
	assert set(t2.keys()) == set('rs')

def all_tests(db):
	test1(db)
	test2(db)

# =============================================================================

def run_redis_test():
	import tkv_redis
	#db = tkv_redis.connect(host='127.0.0.1') # Redis
	db = tkv_redis.connect(host='127.0.0.1', port=8888) # SSDB
	# TODO: test compatibility with KeyDB
	all_tests(db)

def run_sqlite_test():
	import tkv
	db = tkv.connect()
	all_tests(db)

if __name__=="__main__":
	import sys
	if not sys.argv[1:]:
		print('USAGE: test.py sqlite|redis ...', file=sys.stderr)
		exit()
	for arg in sys.argv[1:]:
		fun = {
				'sqlite':run_sqlite_test,
				'redis':run_redis_test,
			}.get(arg)
		if fun:
			print('running',fun)
			fun()
