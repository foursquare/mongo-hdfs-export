// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.hadoop.io

import com.foursquare.common.thrift.bson.TBSONObjectProtocol
import com.foursquare.spindle.{Record, RecordProvider}
import java.io.{BufferedInputStream, InputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred.{FileSplit => HadoopOldFileSplit, RecordReader => OldRecordReader}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.thrift.{TFieldIdEnum, TBase, TException}
import org.bson.{BSONDecoder, BSONObject, BasicBSONDecoder}
import org.slf4j.LoggerFactory

object ThriftBsonRecordReader {
  type TType = TBase[_ <: TBase[_ <: AnyRef, _ <: TFieldIdEnum], _ <: TFieldIdEnum] with Record[_] 
}

class ThriftBsonRecordReader[T <: ThriftBsonRecordReader.TType](val recordProvider: RecordProvider[T],
                                                                configuration: Configuration)
  extends RecordReader[LongWritable, T] with OldRecordReader[LongWritable, T] {
    @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
    // BSON files are not splittable, so the split length is the whole file.
    val decoder: BSONDecoder = new BasicBSONDecoder()
    var totalFileSize: Long = 0
    var rawInputStream: FSDataInputStream = null
    var bufferedIn: BufferedInputStream = null

    // this one is only used for the mapreduce interface
    val curKey: LongWritable = new LongWritable(0L)
    val curValue: T = recordProvider.createRecord

    // MAPREDUCE INTERFACE METHODS
    override def initialize(genericSplit: InputSplit, taskAttemptContext: TaskAttemptContext) =
      initialize(genericSplit, taskAttemptContext.getConfiguration)

    override def nextKeyValue: Boolean = {
      curValue.clear()
      readInto(curKey, curValue)
    }

    override def getCurrentKey: LongWritable = curKey

    override def getCurrentValue: T = curValue

    // satisfies both interfaces
    override def getProgress(): Float = {
      if (totalFileSize == 0) {
        0.0f;
      } else {
        (rawInputStream.getPos() / totalFileSize).asInstanceOf[Float]
      }
    }

    // satisfies both interfaces
    override def close(): Unit = rawInputStream.close()

    // END MAPREDUCE METHODS

    // MAPRED INTERFACE METHODS

    def createKey(): LongWritable = new LongWritable
    def createValue(): T = recordProvider.createRecord

    def getPos(): Long = rawInputStream.getPos()

    def next(key: LongWritable, value: T): Boolean = readInto(key, value)


    // GENERIC METHODS

    private def readInto(key: LongWritable, value: T): Boolean = {
      var hasRead = false
      // Loop until we've either run out of BSON records, or we have
      // a successful read
      while (!hasRead && !endOfStream(bufferedIn)) {
        val srcObject: BSONObject = decoder.readObject(bufferedIn)
        val factory = new TBSONObjectProtocol.ReaderFactory()
        val iprot = factory.getProtocol
        iprot.setSource(srcObject)
        try {
          key.set(getPos())
          // this should work as we have control of the writable. and always
          // initialize it with a Thrift Object.
          value.read(iprot)
          hasRead = true
        } catch {
          case e: TException => logger.error(
            "bad BSON to Thrift translation: " + e.getMessage)
        }
      }
      hasRead
    }

    private def doInitialize(path: Path, fileSize: Long, conf: Configuration) = {
      totalFileSize = fileSize
      val fs = path.getFileSystem(conf)
      val codecFactory = new CompressionCodecFactory(conf)
      val codec = codecFactory.getCodec(path)
      logger.info("Processing file: %s using codec %s".format(path.getName,
        if (codec == null) "null" else codec.getClass.getSimpleName))

      rawInputStream = fs.open(path)
      val stream = if (codec != null) codec.createInputStream(rawInputStream) else rawInputStream
      bufferedIn = new BufferedInputStream(stream)
    }

    def initialize(genericSplit: Any, conf: Configuration) = {
      genericSplit match {
        case fileSplit: FileSplit => doInitialize(fileSplit.getPath, fileSplit.getLength, conf)
        case fileSplit: HadoopOldFileSplit => doInitialize(fileSplit.getPath, fileSplit.getLength, conf)
        case _ => throw new Exception("InputSplit is not a FileSplit: %s".format(genericSplit))
      }
    }

    private def endOfStream(in: InputStream): Boolean = {
      in.mark(1)
      val ret = (in.read == -1)
      in.reset()
      ret
    }
}
