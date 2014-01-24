To run this, copy a mongod executable to this directory. (You can get a copy [here](http://www.mongodb.org/downloads)) Then, run it with `./sbt run <args>`, where args are

* databaseName - the name of the database you are dumping from
* shardName - the shard you are dumping
* inputDir - mongod directory to dump from
* hdfsPath - path to dump data to
* dbPort - any free port for mongod to use
* localTmpDir - local path for temporary data


ThriftBsonInputFormat can be used to read BSON files generated in this way from MapReduce jobs. It's configured with:

```scala
conf.setInputFormat(classOf[ThriftBsonInputFormat])
conf.set(ThriftBsonInputFormat.thriftClass, classOf[MyThriftClass].getName)
```
