package com.rebiekong.bdt.stream.kafka.worker

import com.rebiekong.bdt.stream.commons.RowData
import org.apache.spark.sql.ForeachWriter

class PhoenixWriter(val phoenixURL: String) extends ForeachWriter[RowData] with PhoenixWriteSupport {

  override def open(partitionId: Long, version: Long): Boolean = {
    init(phoenixURL)
    true
  }


  override def process(value: RowData): Unit = save(value)

  override def close(errorOrNull: Throwable): Unit = {
    clean()
  }

}
