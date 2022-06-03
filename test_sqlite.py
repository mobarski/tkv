import tkv
import test

db = tkv.connect(':memory:')
test.test1(db)
t1 = db.table('a')
print(db.tables())
