
# TODO

## NEXT

- stage_items(tab, items) - for testing views -> drop(tab),put_items(tab, items)? set_items? stage_table?
- test_view.txt
- tkv_duckdb.connect_view
- view -> common table expression as source table db.table('users/id/total_value', cte='select id,sum(value) as total_value from user_transactions group by id')
- move tkv_xxx.py to tkv package (tkv.xxx)
- reorder methods in all classes to match TKV

## REST

- rethink sorting of keys and groupping -> keys(sorted=False) ...
- docs - intro
- multicolumn keys in views -> concatws('/',db_key1,db_key2) -> or CTE(concatws(...) as key,\* from ...)
- view class stripping write operations: put, put_many, put_items, drop, delete
- collection interface - key -> many values
- redis - scan_xxx.pattern
- mongo - scan.xxx_pattern
- scan_xxx (pattern) working exactly the same on all DB engines

- docs - api
- tkv_elastic
- tkv_annoy or tkv_faiss interface

## TBD

- limit - optional argument to iterators?
- compression: example, api?
- DB client configuration via single dict and not kwargs?

# DONE

- sqlite -> make single table mode the default
- tkv_duckdb.connect
- tkv_sqlite.connect_view
- split methods with pattern into two: scan_xxx and xxx
- split tkv into tkv and tkv_sqlite
- move this todo list into KANBAN.md

# REJECTED

- table dict interface?
- split TKV into read and write interfaces?
