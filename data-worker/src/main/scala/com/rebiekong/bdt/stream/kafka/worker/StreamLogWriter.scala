package com.rebiekong.bdt.stream.kafka.worker

import java.sql.Types

import com.alibaba.fastjson.JSON
import com.rebiekong.bdt.stream.commons.{ColumnData, RowData}
import org.apache.spark.sql.ForeachWriter

import scala.collection.convert.wrapAll._

class StreamLogWriter(val phoenixURL: String) extends ForeachWriter[RowData] with PhoenixWriteSupport {

  override def open(partitionId: Long, version: Long): Boolean = {
    init(phoenixURL)
    true
  }

  override def process(value: RowData): Unit = {
    val r = new RowData
    r.setSource("STREAM_LOG")
    r.setData(List[ColumnData](
      ColumnData.create("kafka_offset", true, Types.BIGINT, value.getKafkaOffset.toString),
      ColumnData.create("create_time", false, Types.BIGINT, value.getCreateTime.toString),
      ColumnData.create("source", false, Types.VARCHAR, value.getSource),
      ColumnData.create("type", false, Types.VARCHAR, value.getType),
      ColumnData.create("header", false, Types.VARCHAR, JSON.toJSONString(value.getHeader, false)),
      ColumnData.create("data", false, Types.VARCHAR, JSON.toJSONString(value.getData, false)))
    )
    save(r)
  }

  override def close(errorOrNull: Throwable): Unit = {
    clean()
  }


}
