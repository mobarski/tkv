import tkv
import test

db = tkv.connect(':memory:')
test.test1(db)
test.test2(db)
