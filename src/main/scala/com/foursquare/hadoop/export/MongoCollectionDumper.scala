// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.hadoop.export

import com.mongodb.{DB, DBCursor, Mongo, MongoClient, MongoClientOptions, ServerAddress}
import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.net.URI
import java.util.UUID
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory, LzoCodec}
import org.slf4j.LoggerFactory
import scalaj.collection.Imports._


case class FieldFilter(collection: String, field: String)

object Process {
  def spawn(f: =>Unit): Thread = {
    val t = new Thread(new Runnable { def run() = f } )
    t.setDaemon(false)
    t.start()
    t
  }

  def pipeTo(in: InputStream, out: OutputStream): Unit = try {
    val continueCount = 1
    val buffer = new Array[Byte](8192)
    def read {
      val byteCount = in.read(buffer)
      if (byteCount >= continueCount) {
        out.write(buffer, 0, byteCount)
        out.flush()
        read
      }
    }
    try {
      read
    } finally {
      in.close
    }
  } catch { case  _: InterruptedException => () }

}

object TryO {
  def retryAtMost[T](maxNumAttempts: Int, backoffTime: Long = 100)(f: => T): T = {
    retryIf(f) {
      case (_, numAttempts) if numAttempts < maxNumAttempts => {
        Thread.sleep(backoffTime * numAttempts)
        true
      }
      case _ => false
    }
  }

  private def retryIfHelper[T](numAttempts: Int, f: => T, shouldRetry: (Throwable, Int) => Boolean): T = {
    try {
      f
    } catch {
      case ex: java.lang.Error => throw ex  // always throw Error's
      case ex: Throwable => if (shouldRetry(ex, numAttempts)) {
        retryIfHelper(numAttempts + 1, f, shouldRetry)
      } else {
        throw ex
      }
    }
  }

  def retryIf[T](f: => T)(shouldRetry: (Throwable, Int) => Boolean): T = retryIfHelper(1, f, shouldRetry)
}

object MongoCollectionDumper {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  private def newMongodProcess(mongod: String, port: Int, mountDir: String): Process = {
    // start mongod process
    val p = new ProcessBuilder(
      mongod,
      "--port", port.toString,
      "--dbpath", mountDir,
      "--noprealloc",
      "--nohttpinterface",
      "--nounixsocket",
      "--smallfiles"
      ).start
    Process.spawn(try {
      Process.pipeTo(p.getInputStream, System.out)
    } catch { case _: Exception => /* suppress */})
    Process.spawn(try {
      Process.pipeTo(p.getErrorStream, System.err)
    } catch { case _: Exception => /* suppress */})
    p
  }

  def dump(
    port: Int,
    mountDir: String,
    blackListedCollections: Seq[String],
    shardName: String,
    dumpPath: String,
    uploadPath: Option[String],
    conf: Configuration,
    splitSizeBytes: Long,
    dbName: String,
    codec: Option[String] = Some("snappy"),
    mongod: String = "./mongod",
    filters: Seq[FieldFilter] = List(),
    mongoConnectionAttempts: Option[Int] = None
  ): Unit = {
    var retval: Option[Int] = None
    val p = newMongodProcess(mongod, port, mountDir)
    val t = new Thread {
      override def run() = {
        retval = try { Some(p.waitFor()) } finally Thread.interrupted
      }
    }
    t.start()

    try {
      val dumper = MongoCollectionDumper("localhost", port, codec, dbName, mongoConnectionAttempts = mongoConnectionAttempts)
      try {
        require(t.isAlive, "Error starting mongod. retval was: %d".format(retval.getOrElse(
          throw new IllegalStateException("return value not set!"))))

        val unfilteredCollections = dumper.collectionNames
        require(unfilteredCollections.nonEmpty, "No collections found on this shard!")

        val collections = unfilteredCollections.flatMap{ c =>
          // create a message if the collection is being filtered
          if (c.contains("_dlq")) {
            logger.info("-----> skipped - %s".format(c))
            None
          } else if (blackListedCollections.contains(c)) {
            logger.info("-----> skipped - %s - blacklisted".format(c))
            None
          }
          else {
          logger.info("----> %s".format(c))
            Some(c)
          }
        }

        collections.foreach {c =>
          val localPath = new Path(dumpPath, "%s-%s-%s".format(shardName, c, UUID.randomUUID.toString))

          val badFields: Seq[String] = filters.filter(_.collection == c).map(_.field)

          // forcing it to be the local filesystem
          val fs = FileSystem.get(new URI("file:/"), conf)
          if (fs.exists(localPath)) {
            logger.warn("Removing old directory at %s".format(localPath))
            fs.delete(localPath, true)
          }
          logger.info("Writing to %s".format(localPath))
          fs.mkdirs(localPath)
          dumper.dump(c, splitSizeBytes, localPath.toString, shardName, badFields)
          uploadPath.foreach {u =>
            val hdfsPath = new Path(u, "col=%s".format(c))
            val remoteFS = FileSystem.get(hdfsPath.toUri, conf)

            val localFiles = fs.listStatus(localPath).filter{d => !d.isDirectory}.map{d => d.getPath}

            logger.info("Uploading from %s to %s".format(localPath.toString, hdfsPath.toString))
            logger.info("%d files to upload:" format(localFiles.size))
            localFiles.foreach{f => logger.info("---> %s" format(f.toString))}

            // if you don't mkdir, it names all the files the directory name - col=collection
            // which means you only have one file, and it is incorrect.
            remoteFS.mkdirs(hdfsPath)

            // this way we can validate that it actually writes.
            localFiles.foreach{lf =>
              val fullRemotePath = new Path(hdfsPath, lf.getName)
              TryO.retryAtMost(3, 1000) {
                if (remoteFS.exists(fullRemotePath)) {
                  remoteFS.delete(fullRemotePath, true)
                }
                remoteFS.copyFromLocalFile(false, true, lf, fullRemotePath)
              }
              lf.getFileSystem(conf).delete(lf, false)
            }
            // remove the directory once we're done, but only if we're sure that we're done.
            if (fs.listStatus(localPath).size == 0) {
              fs.delete(localPath, true)
            }
          }
          logger.info("Finished processing %s".format(c))
          fs.close()
        }
      } finally {
        dumper.close
      }
    } finally {
      // for some reason this doesn't always work
      p.destroy()
      val cmd: Array[String] = Array(
        "/bin/sh",
        "-c",
        "ps aux | grep mongod | grep %d | awk '{print $2}' | xargs kill -9".format(port)
      )
      Runtime.getRuntime.exec(cmd)
      logger.info("mongod should be dead now.")
      retval match {
        case Some(0) => logger.info("Mongo exited successfully with 0 exit code.")
        case Some(i:Int) => logger.error("Mongo exited with non-zero exit code: %d".format(i))
        case None => logger.error("Couldn't determine mongo exit code.")
      }
    }
  }
}

case class MongoCollectionDumper(
  host: String,
  port: Int,
  codecName: Option[String],
  dbName: String,
  numConnections: Int = 1,
  mongoConnectionAttempts: Option[Int] = None) {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val mongo: Mongo = {
    val options =
      MongoClientOptions.builder()
                        .connectionsPerHost(numConnections)
                        .connectTimeout(5000)
                        .autoConnectRetry(true)
                        .maxAutoConnectRetryTime(20000L)
                        // available as of driver v2.9
                        .cursorFinalizerEnabled(false)
                        .build()
    new MongoClient(new ServerAddress(host, port), options)
  }
  private val db: DB = mongo.getDB(dbName)
  val collectionNames: List[String] = TryO.retryAtMost(mongoConnectionAttempts.getOrElse(3), 10000) {
    db.getCollectionNames().asScala.toList
  }
  logger.info("Connected to %s:%d".format(host, port))

  private val codec: Option[CompressionCodec] = {
    codecName.map {name =>
      val codecFactory = new CompressionCodecFactory(new Configuration)
      val codec = codecFactory.getCodecByName(name)
      require(codec != null, "Could not find %s codec!".format(name))
      logger.info("Using %s".format(codec.getClass.getSimpleName))
      codec
    }
  }

  private case class Stats(
    var bytesWritten: Long = 0,
    var compressedBytesWritten: Long = 0,
    var objectsWritten: Long = 0,
    var fileCount: Int = 0) {

    private var prevFileLength: Long = 0
    def compressedBytesWritten(file: File): Unit = {
      val fileLength = file.length
      if (fileLength < prevFileLength) prevFileLength = 0 // reset
      compressedBytesWritten += fileLength - prevFileLength
      prevFileLength = fileLength
    }

    def log(): Unit = {
      logger.info("Objects written: %d".format(objectsWritten))
      logger.info("Bytes written (compressed): %d".format(compressedBytesWritten))
      logger.info("Bytes written (uncompressed): %d".format(bytesWritten))
      logger.info("Files written: %d".format(fileCount))
    }
  }

  def dump(collName: String, maxChunkSizeBytes: Long, destination: String, shardName: String,
       filterKeys: Seq[String] = Nil): Unit = {
    def makeFile(count: Int): File = {
      new File("%s/%s-%05d.bson%s".format(destination, shardName, count,
          codec.map(c => c match {
            case lzo: LzoCodec => ".lzo"
            case other => other.getDefaultExtension
          }).getOrElse("")))
    }
    def newStream(newFile: File): OutputStream = {
      val fos = new FileOutputStream(newFile)
      codec.map(_.createOutputStream(fos)).getOrElse(fos)
    }
    val stats: Stats = Stats()
    var chunkBytesWritten: Int = 0
    var file: File = makeFile(stats.fileCount)
    val startTime = System.currentTimeMillis
    logger.info("Dumping %s collection to %s".format(collName, destination))
    var os: OutputStream = null
    var cursor: DBCursor = null

    try {
      cursor = db.getCollection(collName).find()

      // intercept the document decoding to do minmal decoding of BSON.
      // this just copies the raw bytes of the document to the OutputStream
      cursor.setDecoderFactory(new MongoStreamingDBDecoderFactory(
        () => os, // OutputStream get function, allows os to be replaced
        (bytesWritten) => { // callback of bytes written for each document
          chunkBytesWritten += bytesWritten
          stats.objectsWritten += 1
          stats.bytesWritten += bytesWritten

          if (chunkBytesWritten > maxChunkSizeBytes) {
            os.close()
            stats.compressedBytesWritten(file)
            logger.debug("Wrote %d bytes to %s".format(chunkBytesWritten, file))
            stats.fileCount += 1
            file = makeFile(stats.fileCount)
            os = newStream(file)
            chunkBytesWritten = 0
          }
        },
        filterKeys
      ))

      os = newStream(file)
      while (cursor.hasNext) {
        cursor.next
      }

      logger.info("Dump of %s took %d ms".format(collName, System.currentTimeMillis - startTime))
    } catch {
      case e: Throwable => {
        logger.error("Dump of %s FAILED.".format(collName), e)
        throw e
      }
    } finally {
      if (os != null) {
        os.close()
      }
      if (cursor != null) cursor.close()
      stats.compressedBytesWritten(file)
      stats.fileCount += 1
      stats.log
    }
  }

  def close = mongo.close()
}

object MongoDump {
  val usage = "MongoDump databaseName shardName inputDir hdfsPath dbPort localTmpDir"

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      throw new IllegalArgumentException(usage)
    }
    val Array(databaseName, shardName, inputDir, dumpPath, dbPort, localTmpDir) = args
    val conf = new Configuration()
    MongoCollectionDumper.dump(
      port = dbPort.toInt,
      mountDir = inputDir,
      blackListedCollections = Seq(),
      dbName = databaseName,
      shardName = shardName,
      dumpPath = localTmpDir,
      uploadPath = Some(dumpPath),
      conf = conf,
      splitSizeBytes = 512 * 1024 * 1024)
  }
}
