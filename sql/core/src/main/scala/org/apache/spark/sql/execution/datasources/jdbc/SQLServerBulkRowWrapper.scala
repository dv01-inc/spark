/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.execution.datasources.jdbc

import collection.JavaConverters._

import scala.collection.immutable.SortedSet

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


class SQLServerBulkRowWrapper(
  rddSchema: StructType,
  dialect: JdbcDialect,
  iterator: Iterator[Row]
) extends ISQLServerBulkRecord {

  private val columns = {
    print("x")
    rddSchema.fields.map { x =>
      val dataType = x.dataType
      val jdbcType = dialect.getJDBCType(dataType)
        .orElse(JdbcUtils.getCommonJDBCType(dataType))
        .getOrElse(
          throw new IllegalArgumentException(s"Can't get JDBC type for ${dataType.simpleString}")
        )
      val (precision, scale) = dataType match {
        case t: DecimalType => (t.precision, t.scale)
        case _ => (0, 0)
      }
      ColumnMetaData(
        dialect.quoteIdentifier(x.name),
        jdbcType.jdbcNullType,
        precision,
        scale
      )
    }
  }

  private val fieldsLength = columns.length

//  // Members declared in java.lang.AutoCloseable
//  def close(): Unit = ???

  // Members declared in com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord
  def getColumnName(i: Int): String = columns(i - 1).name
  def getColumnOrdinals(): java.util.Set[Integer] =
    SortedSet((1 to columns.length).map(i => i: java.lang.Integer): _*).asJava
  def getColumnType(i: Int): Int =
    columns(i - 1).jdbcType
  def getPrecision(i: Int): Int = columns(i - 1).precision
  def getRowData(): Array[Object] =
    SQLServerBulkRowWrapper.rowToObject(iterator.next(), fieldsLength)
  def getScale(i: Int): Int = columns(i - 1).scale
  def isAutoIncrement(x$1: Int): Boolean = false
  def next(): Boolean = iterator.hasNext
}

object SQLServerBulkRowWrapper {
  def rowToObject(r: Row, numFields: Int): Array[Object] = {
    val b = new Array[Object](numFields)
    var i = 0

    while (i < numFields) {
      b(i) = r.get(i).asInstanceOf[Object]
      i = i + 1
    }

    b
  }
}

case class ColumnMetaData(
  name: String,
  jdbcType: Int,
  precision: Int,
  scale: Int
)
