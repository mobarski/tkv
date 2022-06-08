# primitvie as hell but will do for now

import zlib
import json
from time import time
from random import choices,seed,shuffle,random
from tqdm import tqdm

def get_data(records, key_size, val_size, doc=False):
	t0 = time()
	data = []
	keys = []
	for i in range(records):
		key = ''.join(choices("abcdefghij",k=key_size))
		xxx = ''.join(choices("abcdefghij",k=key_size))
		val = xxx*(val_size//key_size)
		if doc:
			data += [(key,{'payload':val})]
		else:
			data += [(key,val)]
		keys += [key]
	print(f'data generated in {time()-t0:0.1f} seconds ({len(keys)} records, {len(set(keys))} unique,  {key_size}+{val_size} bytes each)')
	return keys,data

def benchmark_put(tab, data, keys):
	for k,v in data:
		tab.put(k,v)

def benchmark_get(tab, data, keys):
	for k in keys:
		tab.get(k)

def benchmark_get_items(tab, data, keys):
	list(tab.items())

def benchmark_get_items_prefix(tab, data, keys):
	list(tab.items('e*'))

def benchmark_get_items_suffix(tab, data, keys):
	list(tab.items('*e'))

def benchmark_get_many_2(tab, data, keys):
	for i in range(0,len(keys),2):
		tab.get_many(keys[i:i+2])

def benchmark_get_many_10(tab, data, keys):
	for i in range(0,len(keys),10):
		tab.get_many(keys[i:i+10])

def benchmark_get_many_100(tab, data, keys):
	for i in range(0,len(keys),100):
		tab.get_many(keys[i:i+100])

def benchmark_get_many_250(tab, data, keys):
	for i in range(0,len(keys),250):
		tab.get_many(keys[i:i+250])

def benchmark_get_many_500(tab, data, keys):
	for i in range(0,len(keys),500):
		tab.get_many(keys[i:i+500])


def benchmark(db, fun_list, label='', records=10_000, key_size=10, val_size=1000, iterations=3, tables=3, doc=False, compress=False):
	if compress:
		db.dumps = lambda x:zlib.compress(json.dumps(x).encode('utf8'))
		db.loads = lambda x:json.loads(zlib.decompress(x).decode('utf8'))
	else:
		db.dumps = json.dumps
		db.loads = json.loads
	
	ts = int(time())
	keys,data = [],[]
	for i in range(iterations):
		k,d = get_data(records, key_size, val_size, doc=doc)
		keys += [k]
		data += [d]
	print()

	tab = {}
	for i in range(tables):
		db.drop(f'bench{i}')
		tab[i] = db.table(f'bench{i}')
	
	pg = tqdm(total=tables*iterations*len(fun_list))
	rows = []
	gz = 'gzip:Y' if compress else 'gzip:N'
	t0 = time()
	for it in range(tables):
		for i in range(iterations):
			for fun in fun_list:
				t1 = time()
				fun(tab[it], data[i], keys[i])
				t = time()-t1
				row = label, ts, records, val_size, tables, iterations, gz, it, i, f'{t:5.2f}', str(int(records/t)).rjust(7), fun.__name__.replace('benchmark_','')
				rows += [row]
				pg.update(1)
	del pg
	print()
	# TODO: version
	print(*['label','ts','records','val_size','tables','iterations','gzip','table','iter','time','rows_per_s','benchmark'])
	rows.sort(key=lambda x:(x[-1], x[7], x[8]))
	for row in rows:
		print(*row, sep='  ')
		# TODO: append to file
	print()
	print(f'table_size: {tab[0].size()} chars (compress={compress})')
	print(f'total_time: {time()-t0:0.1f} seconds')

if __name__=="__main__":
	import tkv
	import os

	if 1:
		label = 'sqlite/disk'
		db_path = 'deleteme.sqlite'
		try:
			os.remove(db_path)
		except (FileNotFoundError,OSError):
			pass
	else:
		label = 'sqlite_table/mem'
		db_path = ':memory:'
	db = tkv.connect_table(db_path)
	
	seed(42)
	benchmark(db,
		[
			benchmark_put,
			benchmark_get,
			benchmark_get_items,
			benchmark_get_items_prefix,
			benchmark_get_items_suffix,
			benchmark_get_many_2,
			benchmark_get_many_10,
			benchmark_get_many_100,
			benchmark_get_many_250,
			benchmark_get_many_500,
		],
		label=label,
		records=100_000,
		val_size=100,
		iterations=3,
		tables=3,
		compress=True,
		doc=True,
	)
