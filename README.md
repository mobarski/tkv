# TKV

# Quickstart

```python
import tkv

db = tkv.connect('my_file.sqlite')
db.put('my_table', 'foo', 'bar')
print(db.get('my_table','foo'))

``` 

# Download and Install

Install the latest version with `pip install git+https://github.com/mobarski/tkv.git`.

There are no hard dependencies other than the Python standard library.
