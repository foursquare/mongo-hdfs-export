// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.hadoop.io

import com.foursquare.spindle.RecordProvider
import java.lang.reflect.Modifier
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{FileInputFormat => MapRedFileInputFormat, InputFormat => MapRedInputFormat,
    InputSplit => OldInputSplit, JobConf, RecordReader => OldRecordReader, Reporter}
import org.apache.hadoop.mapreduce.{InputFormat => MapReduceInputFormat, InputSplit, JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapReduceFileInputFormat, FileSplit}

/**
 * ThriftBsonInputFormat can be used to read data from bson data files and deserialize that data to Thrift Objects.
 * The record reader looks at file extensions to determine if the data is compressed, and if so it decompresses before
 * deserializing. BSON files are not splittable (unless indexed), so this InputFormat generates one input split per
 * file.
 *
 * This class implements both the o.a.hadoop.mapred and o.a.hadoop.mapreduce InputFormat interfaces. It contains
 * subclasses of FileInputFormat from both packages and delegates the relevant interfaces methods to those.
 *
 * The class name of the Thrift Object should be specified using the
 * configuration setting ThriftBsonInputFormat.thriftClass. e.g.:
 *
 * conf.setInputFormat(classOf[ThriftBsonInputFormat])
 * conf.set(ThriftBsonInputFormat.thriftClass, classOf[ThriftCheckin].getName)
 *
 * Alternatively, you can subclass ThriftBsonInputFormat and pass along the class type, e.g.:
 *
 * class MyThriftBsonInputFormat extends ThriftBsonInputFormat(Some[classOf[MyThriftClass]]) {}
 *
 */
class ThriftBsonInputFormat[T <: ThriftBsonRecordReader.TType](thriftClass: Option[Class[T]] = None)
  extends MapReduceInputFormat[LongWritable, T] with MapRedInputFormat[LongWritable, T] {

    /** Subclass of the o.a.hadoop.mapreduce package's FileInputFormat */
    val mapreduce = new MapReduceFileInputFormat[LongWritable, T](){
      override def isSplitable(context: JobContext, filename: Path): Boolean = false
      override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
        val conf = context.getConfiguration
        val recordProvider = findRecordProvider(conf, ThriftBsonInputFormat.thriftClass)
        new ThriftBsonRecordReader(recordProvider, conf)
      }
    }

    /** Subclass of the o.a.hadoop.mapred package's FileInputFormat */
    val mapred = new MapRedFileInputFormat[LongWritable, T](){
      override def isSplitable(fs: FileSystem, filename: Path): Boolean = false
      override def getRecordReader(split: OldInputSplit, conf: JobConf,
        reporter: Reporter): OldRecordReader[LongWritable, T]  = {
          val recordProvider = findRecordProvider(conf, ThriftBsonInputFormat.thriftClass)
          val result = new ThriftBsonRecordReader(recordProvider, conf)
          result.initialize(split, conf)
          result
      }
    }

    // NOW TO SATISFY THE INTERFACES

    // MAPRED (old)
    def getRecordReader(split: OldInputSplit, conf: JobConf, reporter: Reporter ) =
      mapred.getRecordReader(split, conf, reporter)

    def getSplits(conf: JobConf, numSplits: Int) = mapred.getSplits(conf, numSplits)

    // MAPREDUCE (new)
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext) =
      mapreduce.createRecordReader(split, context)

    override def getSplits(context: JobContext) = mapreduce.getSplits(context)

    def findRecordProvider(conf: Configuration, prop: String): RecordProvider[T] = {
      val clazz: Class[T] = Option(conf.getClass(prop, null)).getOrElse {
        throw new IllegalArgumentException("No value for prop %s in conf".format(prop))
      }.asInstanceOf[Class[T]]

      // This is a hack to support Raw types. It seems that this is the easiest way to get access to the companion obj
      // for a Raw type, even though it's horrible and circular.
      if (!Modifier.isAbstract(clazz.getModifiers)) {
        clazz.newInstance.meta.asInstanceOf[RecordProvider[T]]
      } else {
        ScalaReflection.objectFromName(clazz.getName, Some(clazz.getClassLoader)).asInstanceOf[RecordProvider[T]]
      }
    }
    
}

object ThriftBsonInputFormat {
  val thriftClass = "thrift.bson.input.format.thrift.class"
}

object ScalaReflection {
  /** Given a name, return the instance of the singleton object for
    * that name.
    */
  def objectFromName(name: String, classLoaderOpt: Option[ClassLoader] = None): AnyRef = {
    val clazz = classLoaderOpt match {
      case None => Class.forName(name + "$")
      case Some(loader) => Class.forName(name + "$", true, loader)
    }
    val moduleGetter = clazz.getDeclaredField("MODULE$")
    moduleGetter.get()
  }
}
