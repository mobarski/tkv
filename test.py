
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
