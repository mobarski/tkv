__author__ = 'Maciej Obarski'
__version__ = '0.6.6'
__license__ = 'MIT'

import itertools
from functools import partial

class TKV:

	# core - read
	
	def get(self, tab, key, default=None):  not_implemented(self, 'get')
	def has(self, tab, key):                not_implemented(self, 'has')
	def size(self, tab):                    not_implemented(self, 'size')
	
	# core - write
	
	def put(self, tab, key, val):           not_implemented(self, 'put')
	def drop(self, tab):                    not_implemented(self, 'drop')
	def delete(self, tab, key):             not_implemented(self, 'delete')
	
	# iterators
	
	def keys(self, tab, sort=False):        not_implemented(self, 'keys')
	def items(self, tab, sort=False):       not_implemented(self, 'items')
	
	def values(self, tab, sort=False):
		return (v for k,v in self.items(tab, sort))
	
	# scan
	
	def scan_keys(self, tab, pattern, sort=False):    not_implemented(self, 'scan_keys')
	def scan_items(self, tab, pattern, sort=False):   not_implemented(self, 'scan_items')
	
	def scan_values(self, tab, pattern, sort=False):
		return (v for k,v in self.scan_items(tab, pattern, sort))
		
	
	
	# extension (can be reimplemented in the child for better performance)
	
	def get_many(self, tab, keys, default=None):
		return [self.get(tab, key, default) for key in keys]
				
	def put_items(self, tab, items):
		for k,v in items:
			self.put(tab, k, v)

	def count(self, tab):
		return len(self.keys(tab))
	
	def scan_count(self, tab, pattern):
		return len(self.scan_keys(tab, pattern))
		
	# sugar

	def get_items(self, tab, keys, default=None):
		return list(zip(keys, self.get_many(tab, keys, default)))

	def put_many(self, tab, keys, values):
		self.put_items(tab, zip(keys, values))
		
	# other

	def table(self, tab, dumps=None, loads=None):
		return KV(tab, self, dumps, loads)

	def tables(self):
		not_implemented(self, 'tables')
	
	def flush(self):
		pass
	
	# ...
	
	def __del__(self):
		self.flush()
		
	@staticmethod
	def group_keys(keys, pos, sep='/'):
		return itertools.groupby(keys, lambda x:sep.join(x.split(sep)[:pos]))


from copy import copy
class KV:
	def __init__(self, tab, tkv, dumps, loads):
		self.dumps = dumps or tkv.dumps
		self.loads = loads or tkv.loads
		_tkv = copy(tkv)
		_tkv.dumps = self.dumps
		_tkv.loads = self.loads
		methods = [x for x in dir(TKV) if x[0]!='_' and x not in ['tables','group_keys','flush','db','tab']]
		for m in methods:
			setattr(self, m, partial(getattr(_tkv, m), tab))

# WIP
class VTKV(TKV):
	"""Virtual TKV - read-only view on existing relational db schema.
	KV interface can access various columns based on information in the table name ie:
	> get('users/uid/name', 42)   # select name    from users  where uid=42
	> get('cities/id/lat,lon', 1) # select lat,lon from cities where id=1
	"""


def not_implemented(self, method_name):
	raise NotImplementedError(f'{self.__class__.__name__}.{method_name}')
