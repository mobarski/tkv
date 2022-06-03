import tkv_redis
import test

# TODO: test Redis compatible engine: KeyDB
# TODO: test Redis compatible engine: Ardb

#db = tkv_redis.connect(host='127.0.0.1') # Redis
db = tkv_redis.connect(host='127.0.0.1', port=8888) # SSDB
test.test1(db)

# ERROR: SSDB db.collections() -> redis.exceptions.ResponseError: wrong number of arguments
