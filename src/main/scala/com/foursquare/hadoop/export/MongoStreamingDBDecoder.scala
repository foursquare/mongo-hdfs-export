// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.hadoop.export

import com.mongodb.{BasicDBObject, DBCallback, DBCollection, DBDecoder, DBDecoderFactory, DBObject}
import java.io.{ByteArrayOutputStream, EOFException, InputStream, OutputStream}
import org.bson.{BSON, BSONCallback, BSONObject}


/**
 * Streaming document decoder to dump unparsed query response bytes to an OutputStream
 * osFunc - function to create an OutputStream.  Called once for each document.
 * traceBytesFunc - callback to trace bytes in document.  Called once for each document after written to OutputStream.
 * filterKeys - keys to exclude from documents
 */
class MongoStreamingDBDecoderFactory(osFunc: () => OutputStream, traceBytesFunc: (Int => Unit),
    filterKeys: Seq[String] = Nil) extends DBDecoderFactory {

  def create(): DBDecoder = new MongoStreamingDBDecoder(osFunc, traceBytesFunc, filterKeys)
}

class MongoStreamingDBDecoder(osFunc: () => OutputStream, traceBytesFunc: (Int => Unit),
    filterKeySeq: Seq[String]) extends DBDecoder {

  // used as intermediate copy buffer
  val buffer: Array[Byte] = new Array[Byte](4096)

  // mongo java driver assumes that decode always returns a doc
  val dummyObject = new BasicDBObject()

  // used for key filtering to buffer the document before writing to the final OutputStream
  // this is done because we won't know the size of the filtered document until it's been fully written
  // and we must write that size at the start of the final OutputStream
  val bufferOs = new ByteArrayOutputStream()

  // readonly byte values of filterKeys
  val filterKeys: Array[Array[Byte]] = filterKeySeq.map(_.getBytes("UTF-8")).toArray

  // buffer for keeping track of which filter keys match the current key
  // reused for each key in a document
  val filterKeyCounters: Array[Int] = new Array[Int](filterKeys.length)

  /**
   * reads keyname from inputStream into buffer.
   * Note: This purposefully not idiomatic scala code.  Doesn't do any allocations
   * returns (shouldFilter, keySize) -- keySize is exclusive of null byte
   */
  def shouldFilter(in: InputStream): (Boolean, Int) = {
    var keyByte = in.read().asInstanceOf[Byte]
    var keyIndex = 0
    // read bytes until we hit a zero
    // compare each byte to the value at the same index within each of the filterKeys
    // if an entire filterKey matches for all bytes read, we should filter
    while (keyByte > 0) {
      // also transfer the byte to the buffer so it gets transferred
      buffer(keyIndex) = keyByte
      // loop through all the filterKeys
      var filterKeyIndex = 0
      while (filterKeyIndex < filterKeys.length) {
        val filterKey = filterKeys(filterKeyIndex)
        // count the number of characters matched for each of the filterKeys
        if (filterKey.length > keyIndex && filterKey(keyIndex) == keyByte) {
          filterKeyCounters(filterKeyIndex) += 1
        } else {
          filterKeyCounters(filterKeyIndex) = 0
        }
        filterKeyIndex += 1
      }
      keyIndex += 1
      keyByte = in.read().asInstanceOf[Byte]
    }

    var shouldFilterKey = false

    // loop through all the filter keys to see if any were a full match
    var filterKeyIndex = 0
    while (filterKeyIndex < filterKeys.length) {
      if (filterKeyCounters(filterKeyIndex) == filterKeys(filterKeyIndex).length) {
        shouldFilterKey = true
      }
      // reset the counters
      filterKeyCounters(filterKeyIndex) = 0
      filterKeyIndex += 1
    }

    (shouldFilterKey, keyIndex)
  }

  def transferManyBytes(in: InputStream, os: OutputStream, count: Int, write: Boolean) {
    var toread = count
    while (toread > 0) {
      val read = in.read(buffer, 0, math.min(toread, buffer.length))
      if (read < 0) {
        throw new EOFException()
      }
      toread -= read
      if (write) {
        os.write(buffer, 0, read)
      }
    }
  }

  def transferInt(in: InputStream, os: OutputStream, write: Boolean): Int = {
    val ch1 = in.read()
    val ch2 = in.read()
    val ch3 = in.read()
    val ch4 = in.read()
    if ((ch1 | ch2 | ch3 | ch4) < 0)
      throw new EOFException()

    if (write) {
      os.write(ch1)
      os.write(ch2)
      os.write(ch3)
      os.write(ch4)
    }
    // little endian
    ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0))
  }

  def transferByte(in: InputStream, os: OutputStream, write: Boolean): Byte = {
    val intValue = in.read()
    if (intValue < 0) {
      throw new EOFException()
    }
    val byteValue = intValue.asInstanceOf[Byte]
    if (write) os.write(byteValue)
    byteValue
  }

  def decode(in: InputStream, collection: DBCollection): DBObject = {
    if (filterKeys.length > 0) {
      filterKeysFromStream(in)
    } else {
      // streaming copy if there's nothing to filter
      val os = osFunc()
      val documentSize = transferInt(in, os, true)
      // already read 4 bytes
      transferManyBytes(in, os, documentSize - 4, true)
      traceBytesFunc(documentSize)
    }

    dummyObject
  }

  /**
   * Copy documents from the stream, but redact keys in in filterKeys
   * see http://bsonspec.org/#/specification for reference
   */
  def filterKeysFromStream(in: InputStream) {
    // reuse the buffer for each document
    bufferOs.reset

    // read size from header, but don't transfer since it will change in the filtered doc
    // subtract 4 because the it's inclusive of the document size
    var toread = MongoStreamingDBDecoder.this.transferInt(in, bufferOs, false) - 4

    // helper methods to stream data copies and decrement the reamining count
    def transferInt(write: Boolean): Int = {
      toread -= 4
      MongoStreamingDBDecoder.this.transferInt(in, bufferOs, write)
    }

    def transferByte(write: Boolean): Byte = {
      toread -= 1
      MongoStreamingDBDecoder.this.transferByte(in, bufferOs, write)
    }

    def transferManyBytes(count: Int, write: Boolean) {
      toread -= count
      MongoStreamingDBDecoder.this.transferManyBytes(in, bufferOs, count, write)
    }

    while (toread > 0) {
      val docType: Byte = transferByte(false)
      if (docType != BSON.EOO) {
        val (shouldFilterKey, keyLength) = shouldFilter(in)
        toread -= (keyLength + 1) // for the null termination
        val shouldWrite = !shouldFilterKey
        if (shouldWrite) {
          bufferOs.write(docType)
          // buffer was populated in shouldFilter
          bufferOs.write(buffer, 0, keyLength)
          bufferOs.write(0) // null terminated cstring
        }
        docType match {
          case BSON.NUMBER | BSON.NUMBER_LONG | BSON.TIMESTAMP | BSON.DATE => // 8 byte
            transferManyBytes(8, shouldWrite)
          case BSON.STRING | BSON.CODE => // int32 (byte*)
            val size = transferInt(shouldWrite)
            transferManyBytes(size, shouldWrite)
          case BSON.OBJECT | BSON.ARRAY => // int32 (byte*) - 4 bytes
            val size = transferInt(shouldWrite)
            transferManyBytes(size - 4, shouldWrite)
          case BSON.BINARY => // int32 byte (byte*)
            val size = transferInt(shouldWrite)
            transferByte(shouldWrite)
            transferManyBytes(size, shouldWrite)
          case BSON.OID => // 12 byte
            transferManyBytes(12, shouldWrite)
          case BSON.BOOLEAN => // byte
            transferByte(shouldWrite)
          case BSON.REGEX => // (byte *) 0x0 (byte*) 0x0
            var byteValue = transferByte(shouldWrite)
            while (byteValue != 0) {
              byteValue = transferByte(shouldWrite)
            }
            byteValue = transferByte(shouldWrite)
            while (byteValue != 0) {
              byteValue = transferByte(shouldWrite)
            }
          case BSON.NUMBER_INT => // int32
            transferInt(shouldWrite)
          case BSON.NULL | BSON.MINKEY | BSON.MAXKEY => // 0 byte
          case _ => throw new UnsupportedOperationException("Invalid bson type " + docType)
        }
      } else {
        bufferOs.write(BSON.EOO)
      }
    }

    // add 4 bytes to account for document size
    val totalBytesWritten = bufferOs.size + 4
    val os = osFunc()

    os.write(totalBytesWritten >> 0)
    os.write(totalBytesWritten >> 8)
    os.write(totalBytesWritten >> 16)
    os.write(totalBytesWritten >> 24)

    bufferOs.writeTo(os)

    traceBytesFunc(totalBytesWritten)
  }

  def getDBCallback(collection: DBCollection): DBCallback = {
    throw new UnsupportedOperationException("Not implemented")
  }
  def decode(bytes: Array[Byte], collection: DBCollection): DBObject = {
    throw new UnsupportedOperationException("Not implemented")
  }
  def readObject(bytes: Array[Byte]): BSONObject = {
    throw new UnsupportedOperationException("Not implemented")
  }
  def readObject(in: InputStream): BSONObject = {
    throw new UnsupportedOperationException("Not implemented")
  }
  def decode(bytes: Array[Byte], callback: BSONCallback): Int = {
    throw new UnsupportedOperationException("Not implemented")
  }
  def decode(in: InputStream, callback: BSONCallback): Int = {
    throw new UnsupportedOperationException("Not implemented")
  }
}
