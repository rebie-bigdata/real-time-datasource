package com.rebiekong.bdt.stream.kafka.worker

import java.sql.{Connection, DatabaseMetaData, DriverManager, Types}

import com.rebiekong.bdt.stream.commons.{ColumnData, RowData}
import org.apache.phoenix.schema.types.PDataType

import scala.collection.convert.wrapAsScala._

trait PhoenixWriteSupport {

  private var connection: Connection = _
  private var meta: DatabaseMetaData = _

  protected def init(phoenixURL: String): Unit = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    connection = DriverManager.getConnection(phoenixURL)
    meta = connection.getMetaData
  }

  protected def clean(): Unit = {
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  def save(rowData: RowData): Unit = {
    rowData.getType.toLowerCase match {
      case "delete" => deleteData(rowData.getSource, rowData.getData.toList)
      case _: String =>
        saveData(rowData.getSource, rowData.getData.toList)
    }
  }

  private def modifyStruct(table: String, data: List[ColumnData]): Unit = {
    if (!tableExist(table)) {
      createTable(table, data)
    } else {
      alterTable(table, data)
    }
  }

  private def tableExist(tableName: String): Boolean = {
    val tc = tableName.toUpperCase().split('.')
    val scheme = if (tc.length < 2) null else tc(tc.length - 2)
    val table = tc(tc.length - 1)
    val ts = meta.getTables(null, scheme, table, Array[String]("TABLE"))
    try {
      ts.next()
      ts.getString(3)
      true
    } catch {
      case _: Throwable => false
    }
  }

  private def alterTable(table: String, newColumn: List[ColumnData]): Unit = {
    val tMetaData = connection.createStatement().executeQuery(s"SELECT * FROM $table WHERE 0=1").getMetaData
    val existFields = (1 until tMetaData.getColumnCount).map(tMetaData.getColumnName)
    val fields = newColumn.map(columnData => columnData.getName).distinct
    val needToAlter = {
      fields.foreach(ff => {
        if (!existFields.contains(ff)) true
      })
      false
    }
    if (needToAlter) {
      val columnDefs = newColumn.map(column => toFieldString(column.getName, column.getType, column.isPrimaryKey))
      val sql = s"ALTER TABLE $table ADD IF NOT EXISTS " + columnDefs.mkString(",")
      connection.createStatement().execute(sql)
    }
  }

  private def createTable(table: String, newColumn: List[ColumnData]): Unit = {
    val columnDefs = newColumn.map(column => toFieldString(column.getName, column.getType, column.isPrimaryKey))
    val primaryKeys = newColumn
      .map(columnData => (columnData.getName, columnData.isPrimaryKey))
      .distinct
      .filter(t => t._2).map(t => t._1)

    val sql = s"CREATE TABLE IF NOT EXISTS $table (" +
      columnDefs.mkString(",") +
      " CONSTRAINT my_pk PRIMARY KEY(" + primaryKeys.mkString(",") + ")" +
      ") SALT_BUCKETS=16,APPEND_ONLY_SCHEMA=TRUE,UPDATE_CACHE_FREQUENCY=NEVER"

    connection.createStatement.execute(sql)
  }

  private def deleteData(table: String, data: List[ColumnData]): Unit = {
    val pks = data.filter(column => column.isPrimaryKey).map(column => fixFieldName(column.getName) + "=" + fixValue(column.getValue, column.getType))
    val stm = connection.createStatement()
    val s = s"DELETE FROM $table WHERE " + pks.mkString(" AND ")
    println(s)
    try {
      stm.execute(s)
    } catch {
      case e: Throwable => throw e
    } finally {
      connection.commit()
    }
  }

  private def saveData(table: String, data: List[ColumnData]): Unit = {
    modifyStruct(table, data)
    val stm = connection.createStatement()
    val (k, v) = data.map(columnData => (columnData.getName, (columnData.getValue, columnData.getType))).unzip
    val s = s"UPSERT INTO $table (" + k.map(fixFieldName).mkString(",") + ") VALUES (" + v.map(i => fixValue(i._1, i._2)).mkString(",") + ")"
    println(s)
    try {
      stm.execute(s)
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      connection.commit()
    }
  }

  private def toFieldString(name: String, sqlType: Int, isPrimaryKey: Boolean): String = {
    "%s %s%s".format(
      fixFieldName(name)
      , PDataType.fromTypeId({
        sqlType match {
          case Types.BIT => Types.BIT
          case Types.TINYINT => Types.TINYINT
          case Types.SMALLINT => Types.SMALLINT
          case Types.INTEGER => Types.INTEGER
          case Types.BIGINT => Types.BIGINT
          case Types.FLOAT => Types.FLOAT
          case Types.REAL => Types.FLOAT
          case Types.DOUBLE => Types.DOUBLE
          case Types.NUMERIC => Types.NUMERIC
          case Types.DECIMAL => Types.DECIMAL
          case Types.CHAR => Types.VARCHAR
          case Types.VARCHAR => Types.VARCHAR
          case Types.LONGVARCHAR => Types.VARCHAR
          case Types.DATE => Types.VARCHAR
          case Types.TIME => Types.VARCHAR
          case Types.TIMESTAMP => Types.TIMESTAMP
          case Types.BINARY => Types.BINARY
          case Types.VARBINARY => Types.VARBINARY
          case Types.LONGVARBINARY => Types.VARCHAR
          case Types.NULL => Types.NULL
          case Types.OTHER => Types.OTHER
          case Types.JAVA_OBJECT => Types.JAVA_OBJECT
          case Types.DISTINCT => Types.DISTINCT
          case Types.STRUCT => Types.STRUCT
          case Types.ARRAY => Types.ARRAY
          case Types.BLOB => Types.VARCHAR
          case Types.CLOB => Types.VARCHAR
          case Types.REF => Types.REF
          case Types.DATALINK => Types.DATALINK
          case Types.BOOLEAN => Types.BOOLEAN
          case Types.ROWID => Types.ROWID
          case Types.NCHAR => Types.NCHAR
          case Types.NVARCHAR => Types.NVARCHAR
          case Types.LONGNVARCHAR => Types.LONGNVARCHAR
          case Types.NCLOB => Types.NCLOB
          case Types.SQLXML => Types.SQLXML
          case Types.REF_CURSOR => Types.REF_CURSOR
          case Types.TIME_WITH_TIMEZONE => Types.TIME_WITH_TIMEZONE
          case Types.TIMESTAMP_WITH_TIMEZONE => Types.TIMESTAMP_WITH_TIMEZONE
          case _ => Types.VARCHAR
        }
      }).getSqlTypeName, if (isPrimaryKey) {
        " NOT NULL"
      } else {
        ""
      })
  }

  def fixFieldName(name: String): String = {
    name match {
      case "key" => "\"KEY\""
      case "value" => "\"VALUE\""
      case i: String => i
    }
  }

  def fixValue(value: String, sqlType: Int): String = {

    val numericFunc = (v: String) => if (value == null || value.length == 0) "null" else v
    val stringFunc = (v: String) => {
      val v2 = v.replace("'", "\\'")
      s"'$v2'"
    }
    val timestampFunc = (v: String) => if (value != null && value.equals("0000-00-00 00:00:00")) "null" else s"'$v'"
    val f = sqlType match {
      case Types.BIT => numericFunc
      case Types.TINYINT => numericFunc
      case Types.SMALLINT => numericFunc
      case Types.INTEGER => numericFunc
      case Types.BIGINT => numericFunc
      case Types.FLOAT => numericFunc
      case Types.REAL => numericFunc
      case Types.DOUBLE => numericFunc
      case Types.NUMERIC => numericFunc
      case Types.DECIMAL => numericFunc
      case Types.CHAR => stringFunc
      case Types.VARCHAR => stringFunc
      case Types.LONGVARCHAR => stringFunc
      case Types.DATE => stringFunc
      case Types.TIME => stringFunc
      case Types.TIMESTAMP => timestampFunc
      case Types.BINARY => stringFunc
      case Types.VARBINARY => stringFunc
      case Types.LONGVARBINARY => stringFunc
      case Types.NULL => stringFunc
      case Types.OTHER => stringFunc
      case Types.JAVA_OBJECT => stringFunc
      case Types.DISTINCT => stringFunc
      case Types.STRUCT => stringFunc
      case Types.ARRAY => stringFunc
      case Types.BLOB => stringFunc
      case Types.CLOB => stringFunc
      case Types.REF => stringFunc
      case Types.DATALINK => stringFunc
      case Types.BOOLEAN => stringFunc
      case Types.ROWID => stringFunc
      case Types.NCHAR => stringFunc
      case Types.NVARCHAR => stringFunc
      case Types.LONGNVARCHAR => stringFunc
      case Types.NCLOB => stringFunc
      case Types.SQLXML => stringFunc
      case Types.REF_CURSOR => stringFunc
      case Types.TIME_WITH_TIMEZONE => stringFunc
      case Types.TIMESTAMP_WITH_TIMEZONE => stringFunc
      case _ => stringFunc
    }
    f.apply(value)
  }
}
