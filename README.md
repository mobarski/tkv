# TKV

**Warning**: this is pre-alpha code - anything can change without a warning.

# Quickstart

**Basics**:
```python
import tkv_sqlite

db = tkv_sqlite.connect('my_file.sqlite')

# simple usage
db.put('my_table', 'foo', 'hello')
x = db.get('my_table', 'foo') # x = 'hello'

# table object
tab = db.table('my_table')
x = tab.get('foo')     # x = 'hello'
y = tab.get('bar', -1) # y = -1
z = tab.count()        # z = 1

# document store
tab.put('my_doc', {'foo':'hello', 'bar':1.23, 'baz':[1,2,3]})
x = tab.get('my_doc')['foo'] # x = 'hello'
``` 

**Connecting to other db engines:**
```python
import tkv_redis
db = tkv_redis.connect(host='127.0.0.1')

import tkv_mongo
db = tkv_mongo.connect('127.0.0.1', db='tkv')
```

**Other operations:**
```python
db.put_items('my_table', [('foo',1),('bar',2)])

db.keys('my_table')   # -> iter(['bar', 'foo'])
db.items('my_table')  # -> iter([('bar',2), ('foo',1)])
db.values('my_table') # -> iter([2,1])

db.put_many('my_table', ['baz','qux'], [3,4])
vals = db.get_many('my_table', ['foo','qux','xyzzy'], -1) # vals = [1, 4, -1]

db.delete('my_table', 'foo')
db.drop('my_table')
```

**Scan operations:**
```python
tab = db.table('xyz')
# ------ x y z <-------- key part name
# ------ 0 1 2 <-------- key part index
tab.put('A/1/1', 'foo')
tab.put('A/1/2', 'bar')
tab.put('A/2/1', 'baz')
tab.put('B/2/4', 'qux')
tab.put('B/3/6', '...')

tab.scan_keys('A/1/*') # -> iter(['A/1/1', 'A/1/2'])
tab.scan_values('A/*') # -> iter(['foo', 'bar, 'baz'])
tab.scan_items('B/*')  # -> iter([('B/2/4','qux'), ('B/3/6','...')])

```

**Partition by key fragment:**
```python	
keys = db.keys('xyz')
for x,keys_x in db.group_keys(keys, 0):        # key part index=0 (x)
    print(x, *keys_x)
    for y,keys_y in db.group_keys(keys_x, 1):  # key part index=1 (y)
        print(y, *keys_y)

# prints:
#   A A/1/1 A/1/2 A/2/1
#   1 A/1/1 A/1/2
#   2 A/2/1
#   B B/2/4 B/3/6
#   2 B/2/4
#   3 B/3/6
```

**Advanced usage - custom serializer/deserializer**:
```python
import pickle

db = tkv_sqlite.connect('my_pickle_db.sqlite',
    dumps = pickle.dumps,
    loads = pickle.loads,
)

a = set("abcd")
b = set("axcy")

tab = db.table('foo')
tab.put('a', a)
tab.put('b', b)

x = a & b                       # x = {'c', 'a'}
y = tab.get('a') & tab.get('b') # y = {'c', 'a'}

```

**Advanced usage - table specific serializer/deserializer:**
```python
import cloudpickle

tab = db.table('my_functions',
    dumps = cloudpickle.dumps,
    loads = cloudpickle.loads,
)

tab.put('foo', lambda x:x*2)
x = tab.get('foo')(21) # x = 42
```

# Download and Install

Install the latest version with:
```
pip install git+https://github.com/mobarski/tkv
```

There are no hard dependencies other than the Python standard library.

**Important:** db connectors are not defined as package dependencies.
