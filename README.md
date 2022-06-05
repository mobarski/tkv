# TKV

**Warning**: this is pre-alpha code - anything can change without a warning.

# Quickstart

```python
import tkv

db = tkv.connect('my_file.sqlite')

# simple usage
db.put('my_table', 'foo', 'bar')
x = db.get('my_table', 'foo')
y = db.get('my_table', 'baz', -1)

# table example
tab = db.table('my_table')
x = tab.get('foo')
y = tab.get('baz', -1)

# document store
tab.put('my_doc', {'hello':'world'})

``` 

# Download and Install

Install the latest version with `pip install git+https://github.com/mobarski/tkv.git`.

There are no hard dependencies other than the Python standard library.
