package com.rebiekong.bdt.stream.kafka

import com.alibaba.fastjson.JSON
import com.rebiekong.bdt.stream.commons.RowData
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

private[kafka] class Reader() {

  private val kafkaOptions: mutable.Map[String, String] = mutable.Map.empty[String, String]
  private val sinks: ListBuffer[StreamWorker] = ListBuffer.empty[StreamWorker]

  def run(spark: SparkSession): Unit = {
    val mdf: Dataset[RowData] = df(spark, kafkaOptions.toMap)
    sinks.foreach(worker => {
      val dfWriter = mdf.writeStream
        .trigger(worker.getTrigger)
        .options(worker.getOptions)

      val fdfWriter = worker.getFormat match {
        case "console" => dfWriter.format("console")
        case "foreach" => dfWriter.foreach(worker.getForeachWriter)
        case _ => throw new Exception("NO SUPPORT!")
      }
      fdfWriter.start()
    })
    spark.streams.awaitAnyTermination()
  }

  private def df(spark: SparkSession, kafkaOptions: Map[String, String]): Dataset[RowData] = {
    import spark.sqlContext.implicits._
    spark.readStream
      .format("kafka")
      .options(kafkaOptions)
      .load()
      .select(
        $"value" cast DataTypes.StringType as "value",
        $"offset"
      ).map(new MapFunction[Row, RowData] {
      override def call(t: Row): RowData = {
        val j = JSON.parseObject(t.getString(0), classOf[RowData])
        j.setKafkaOffset(t.getLong(1))
        j
      }
    }, Encoders.bean(classOf[RowData]))
  }

  private def addSink(sink: StreamWorker*): Unit = sinks.appendAll(sink)

  private def options(options: Map[String, String]): Unit = options.toList.foreach(item => kafkaOptions += item)
}

object Reader {

  def builder: ReaderBuilder = {
    new ReaderBuilder()
  }

  private[kafka] class ReaderBuilder {

    private val options: mutable.Map[String, String] = mutable.Map.empty[String, String]
    private val sinks: ListBuffer[StreamWorker] = ListBuffer.empty[StreamWorker]

    def options(seq: Map[String, String]): ReaderBuilder = {
      seq.foreach(i => option(i._1, i._2))
      this
    }

    def option(key: String, value: String): ReaderBuilder = {
      options += key -> value
      this
    }

    def assign(value: String): ReaderBuilder = {
      option("assign", value)
    }


    def subscribe(topics: String): ReaderBuilder = {
      option("subscribe", topics)
    }

    def subscribePattern(pattern: String): ReaderBuilder = {
      option("subscribePattern", pattern)
    }

    def kafkaServers(servers: String): ReaderBuilder = {
      option("kafka.bootstrap.servers", servers)
    }

    def build(): Reader = {
      val t = new Reader()
      t.options(options.toMap)
      t.addSink(sinks: _*)
      t
    }

    def addSink(sink: StreamWorker*): ReaderBuilder = {
      sinks.appendAll(sink)
      this
    }
  }

}
