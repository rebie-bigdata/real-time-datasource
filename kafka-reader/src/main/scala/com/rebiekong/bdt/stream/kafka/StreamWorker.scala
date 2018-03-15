package com.rebiekong.bdt.stream.kafka

import com.rebiekong.bdt.stream.commons.RowData
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

private class StreamWorker() {

  private var queryName: String = _
  private var trigger: Trigger = _
  private var outputMode: OutputMode = _
  private var foreachWriter: ForeachWriter[RowData] = _
  private var options: Map[String, String] = _
  private var path: String = _
  private var format: String = _

  def getQueryName: String = queryName

  private[kafka] def setQueryName(queryName: String): Unit = this.queryName = queryName

  def getTrigger: Trigger = trigger

  private[kafka] def setTrigger(trigger: Trigger): Unit = this.trigger = trigger

  def getOutputMode: OutputMode = outputMode

  private[kafka] def setOutputMode(outputMode: OutputMode): Unit = this.outputMode = outputMode

  def getForeachWriter: ForeachWriter[RowData] = foreachWriter

  private[kafka] def setForeachWriter(foreachWriter: ForeachWriter[RowData]): Unit = this.foreachWriter = foreachWriter

  def getOptions: Map[String, String] = options

  private[kafka] def setOptions(options: Map[String, String]): Unit = this.options = options

  def getPath: String = path

  private[kafka] def setPath(options: String): Unit = this.path = path

  def getFormat: String = format

  private[kafka] def setFormat(format: String): Unit = this.format = format


}

object StreamWorker {

  private[kafka] class StreamWorkerBuilder {

    private var queryName: String = _
    private var trigger: Trigger = Trigger.ProcessingTime(0)
    private var outputMode: OutputMode = OutputMode.Append()
    private var foreachWriter: ForeachWriter[RowData] = _
    private var options: Map[String, String] = Map.empty[String, String]
    private var path: String = _
    private var format: String = _

    def setQueryName(queryName: String): StreamWorkerBuilder = {
      this.queryName = queryName
      this
    }

    def setTrigger(trigger: Trigger): StreamWorkerBuilder = {
      this.trigger = trigger
      this
    }

    def setOutputMode(outputMode: OutputMode): StreamWorkerBuilder = {
      this.outputMode = outputMode
      this
    }

    private[kafka] def setForeachWriter(foreachWriter: ForeachWriter[RowData]): StreamWorkerBuilder = {
      this.foreachWriter = foreachWriter
      this
    }

    def setOptions(options: Map[String, String]): StreamWorkerBuilder = {
      options.foreach(i => this.options += i)
      this
    }

    def setOption(key: String, value: String): StreamWorkerBuilder = {
      this.options += key -> value
      this
    }

    private[kafka] def setPath(options: String): StreamWorkerBuilder = {
      this.path = path
      this
    }

    private[kafka] def setFormat(format: String): StreamWorkerBuilder = {
      this.format = format
      this
    }

    def build(): StreamWorker = synchronized({
      val worker = new StreamWorker
      if (queryName != null) {
        worker.setQueryName(queryName)
      } else {
        exception("name must exists")
      }
      if (trigger != null) {
        worker.setTrigger(trigger)
      }
      if (foreachWriter != null) {
        worker.setForeachWriter(foreachWriter)
      }
      if (format != null) {
        worker.setFormat(format)
      } else {
        exception("format must exists")
      }
      if (path != null) {
        worker.setPath(path)
      }
      if (outputMode != null) {
        worker.setOutputMode(outputMode)
      }
      if (options != null) {
        worker.setOptions(options)
      }
      worker
    })


    private def exception(msg: String = ""): Unit = {
      throw new Exception(msg)
    }
  }

  object builder {
    // currently, you just can create the foreach worker because I JUST USING IT!
    def foreach(foreachWriter: ForeachWriter[RowData]): StreamWorkerBuilder = {
      val t = new StreamWorkerBuilder
      t.setForeachWriter(foreachWriter)
      t.setFormat("foreach")
      t
    }

    // FOR TEST
    def console(): StreamWorkerBuilder = {
      val t = new StreamWorkerBuilder
      t.setFormat("console")
      t
    }
  }


}
