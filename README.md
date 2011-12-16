# fkv
fkv is a fast key-value store, can used in embedded scene.

## feauture
- fixed record size, key length and value length when create new Fkv database.
- very quick write/read speed, 20,0000 put/second,100,0000 get/second in my macbook466(hdd) with 8 byte size key and 10 byte size value.
- small db size, because of fixed key/value length, new input record will reuse the deleted space first, and then append in the end if no deleted space

## architect
![architect](/doc/img/fkv.png "fkv architect")


# exmaple
```java		
	File dbFile = new File("/tmp/fkvtest.db"); 
	// load old database or create new one(if file not exists) witch can store 10000 records  
	Fkv fkv = new FkvImpl(dbFile, 10000, 8, 10); 
	// key must be 8 byte size
	String key = "01234567"; 
	// value must be 10 byte size
	String value = "0123456789"; 
	fkv.put(key, value);
	fkv.delete(key);
	fkv.get(key);
	fkv.close();
```

# community
weibo.com/seanlinwang  xailnx@gmail.com