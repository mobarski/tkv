# TKV

# Quickstart

```python
import tkv

db = tkv.connect('my_file.sqlite')

db.put('my_table', 'foo', 'bar')
x = db.get('my_table', 'foo')
y = db.get('my_table', 'baz', -1)

db.put('other_table', 'the question', {'answer':42})

tab = db.table('my_table')
x = tab.get('foo')
y = tab.get('baz', -1)

``` 

# Download and Install

Install the latest version with `pip install git+https://github.com/mobarski/tkv.git`.

There are no hard dependencies other than the Python standard library.
