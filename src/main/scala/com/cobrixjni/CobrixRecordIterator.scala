package com.cobrixjni

import com.fasterxml.jackson.databind.ObjectMapper

import java.io.{BufferedInputStream, FileInputStream}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class CobrixRecordIterator(copybook: AnyRef,
                          fieldNames: Seq[String],
                          dataPath: String,
                          recordLength: Int,
                          batchSize: Int) {
  require(recordLength > 0, s"recordLength must be > 0, got $recordLength")
  require(batchSize > 0, s"batchSize must be > 0, got $batchSize")

  private val mapper = new ObjectMapper()
  private val in = new BufferedInputStream(new FileInputStream(dataPath))

  def nextBatch(): Seq[String] = {
    val out = ArrayBuffer.empty[String]
    while (out.length < batchSize) {
      val rec = readRecord(recordLength)
      if (rec.isEmpty) return out.toSeq
      out += decodeToJson(rec.get)
    }
    out.toSeq
  }

  def close(): Unit = in.close()

  private def readRecord(length: Int): Option[Array[Byte]] = {
    val buf = new Array[Byte](length)
    var total = 0
    while (total < length) {
      val n = in.read(buf, total, length - total)
      if (n < 0) {
        return if (total == 0) None else Some(buf.take(total))
      }
      total += n
    }
    Some(buf)
  }

  private def decodeToJson(record: Array[Byte]): String = {
    val decoded = fieldNames.map { name =>
      val value = Try {
        copybook
          .getClass
          .getMethod("getFieldValueByName", classOf[String], classOf[Array[Byte]], classOf[Int])
          .invoke(copybook, name, record, Int.box(0))
      }.toOption.orNull
      name -> (if (value == null) null else value.toString)
    }.toMap

    mapper.writeValueAsString(decoded)
  }
}
