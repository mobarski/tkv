# TKV

**Warning**: this is pre-alpha code - anything can change without a warning.

# Quickstart

```python
import tkv

db = tkv.connect('my_file.sqlite')

# simple usage
db.put('my_table', 'foo', 'hello')
x = db.get('my_table', 'foo')     # -> 'hello'
y = db.get('my_table', 'bar', -1) # -> -1
z = db.count('my_table')          # -> 1

# table example
tab = db.table('my_table')
x = tab.get('foo')     # -> 'hello'
y = tab.get('bar', -1) # -> -1
z = tab.count()        # -> 1

# document store
tab.put('my_doc', {'foo':'hello', 'bar':1.23, 'baz':[1,2,3]})
x = tab.get('my_doc')['foo'] # -> 'hello'
``` 

```python
import tkv_redis
db = tkv_redis.connect(host='127.0.0.1')

import tkv_mongo
db = tkv_mongo.connect('127.0.0.1')
```

# Download and Install

Install the latest version with `pip install git+https://github.com/mobarski/tkv.git`.

There are no hard dependencies other than the Python standard library.
