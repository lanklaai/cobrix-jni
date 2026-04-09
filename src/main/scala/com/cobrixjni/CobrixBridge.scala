package com.cobrixjni

import com.fasterxml.jackson.databind.ObjectMapper

import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

class CobrixBridge {
  private val mapper = new ObjectMapper()
  private val handles = new ConcurrentHashMap[Long, CobrixRecordIterator]()
  private val idGen = new AtomicLong(1L)

  def schemaJson(copybookPath: String): String = {
    val copybook = copybookFromPath(copybookPath)
    val layout = callNoArg(copybook, "generateRecordLayoutPositions").toString
    val fields = extractFieldNamesFromLayout(layout)
    val recordLength = detectRecordLength(copybook, layout)
    mapper.writeValueAsString(
      Map(
        "layout" -> layout,
        "fields" -> fields,
        "record_length" -> recordLength
      ).asJava
    )
  }

  def openReader(copybookPath: String, dataPath: String, batchSize: Int): Long = {
    val copybook = copybookFromPath(copybookPath)
    val layout = callNoArg(copybook, "generateRecordLayoutPositions").toString
    val fields = extractFieldNamesFromLayout(layout)
    val recordLength = detectRecordLength(copybook, layout)

    val it = new CobrixRecordIterator(copybook, fields, dataPath, recordLength, batchSize)
    val id = idGen.getAndIncrement()
    handles.put(id, it)
    id
  }

  def nextBatchJson(handle: Long): Array[String] = {
    val it = handles.get(handle)
    if (it == null) return null
    val batch = it.nextBatch()
    if (batch.isEmpty) {
      handles.remove(handle)
      null
    } else {
      batch.toArray
    }
  }

  def closeReader(handle: Long): Unit = {
    val it = handles.remove(handle)
    if (it != null) {
      it.close()
    }
  }

  private def copybookFromPath(copybookPath: String): AnyRef = {
    val copybookContents = Files.readString(Paths.get(copybookPath))
    val parserModule = Class.forName("za.co.absa.cobrix.cobol.parser.CopybookParser$")
      .getField("MODULE$")
      .get(null)
    parserModule
      .getClass
      .getMethod("parseSimple", classOf[String])
      .invoke(parserModule, copybookContents)
      .asInstanceOf[AnyRef]
  }

  private def callNoArg(instance: AnyRef, method: String): AnyRef =
    instance.getClass.getMethod(method).invoke(instance).asInstanceOf[AnyRef]

  private def detectRecordLength(copybook: AnyRef, layout: String): Int = {
    val candidateMethods = Seq("getRecordSize", "recordSize", "binarySize", "getBinarySize")
    candidateMethods.view
      .flatMap { m =>
        scala.util.Try(copybook.getClass.getMethod(m).invoke(copybook)).toOption
      }
      .collectFirst {
        case i: Integer => i.intValue()
        case l: java.lang.Long => l.intValue()
      }
      .getOrElse(
        extractMaxEndOffset(layout)
      )
  }

  private def extractFieldNamesFromLayout(layout: String): Seq[String] = {
    layout
      .split("\\r?\\n")
      .iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(line => line.exists(_.isLetter))
      .map { line =>
        line.takeWhile(ch => ch != ' ' && ch != '(' && ch != '[')
      }
      .filter(_.nonEmpty)
      .toSeq
      .distinct
  }

  private def extractMaxEndOffset(layout: String): Int = {
    val range = """(\d+)\s*[-:]\s*(\d+)""".r
    range.findAllMatchIn(layout)
      .map(m => m.group(2).toInt)
      .foldLeft(0)(Math.max)
  }
}
