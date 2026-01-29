package com.portofino.arrow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.sql.DriverManager
import java.util.Properties

/**
 * Arrow Flight SQL 测试程序
 *
 * 测试通过 Arrow Flight SQL JDBC 驱动连接 Doris 并读取数据
 *
 * @author jiangxintong@chinamobile.com 2026/1/28
 */
object ArrowFlightSQLSuite {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Arrow Flight SQL Test")
    println("=" * 60)

    val spark = SparkSession
      .builder()
      .appName("ArrowFlightSQLTest")
      .master("local[4]")
      .getOrCreate()

    try {
      // 从配置读取参数
      val conf = spark.sparkContext.getConf
      val host = conf.get("arrow.host", "localhost")
      val port = conf.get("arrow.port", "8070")
      val user = conf.get("arrow.user", "root")
      val password = conf.get("arrow.password", "")
      val query = conf.get("arrow.query", "SELECT 1")

      println(s"Host: $host:$port")
      println(s"User: $user")
      println(s"Query: $query")
      println()

      // 测试 JDBC 连接
      testJdbcConnection(host, port, user, password, query)

      // 测试 Spark JDBC 读取
      println("\n" + "=" * 60)
      println("Testing Spark JDBC Reader")
      println("=" * 60)
      testSparkJdbcReader(spark, host, port, user, password, query)

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  /**
   * 测试直接 JDBC 连接
   */
  def testJdbcConnection(host: String, port: String, user: String, password: String, query: String): Unit = {
    println("Testing Direct JDBC Connection...")

    // 加载驱动
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")

    // 构建连接 URL
    val url = s"jdbc:arrow-flight-sql://$host:$port" +
      "?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false"

    println(s"URL: $url")

    // 创建连接属性
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    // 连接并查询
    val connection = DriverManager.getConnection(url, props)
    try {
      println("✓ Connection established")

      val statement = connection.createStatement()
      try {
        println(s"Executing query: $query")
        val resultSet = statement.executeQuery(query)
        val metadata = resultSet.getMetaData
        val columnCount = metadata.getColumnCount

        // 打印列信息
        println("\nColumns:")
        for (i <- 1 to columnCount) {
          println(s"  ${metadata.getColumnName(i)}: ${metadata.getColumnTypeName(i)}")
        }

        // 打印数据
        println("\nData:")
        var rowCount = 0
        while (resultSet.next() && rowCount < 20) {
          val values = (1 to columnCount).map { i =>
            val value = resultSet.getObject(i)
            if (resultSet.wasNull()) "NULL" else value.toString
          }
          println(s"  ${values.mkString(", ")}")
          rowCount += 1
        }

        println(s"\n✓ Query executed successfully (showing first $rowCount rows)")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  /**
   * 测试 Spark JDBC 读取
   */
  def testSparkJdbcReader(spark: SparkSession, host: String, port: String,
                          user: String, password: String, query: String): Unit = {
    println("Testing Spark JDBC Reader...")

    val url = s"jdbc:arrow-flight-sql://$host:$port" +
      "?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false"

    println(s"URL: $url")

    try {
      val df = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
        .option("user", user)
        .option("password", password)
        .option("query", query)
        .load()

      println("✓ DataFrame created")
      println("\nSchema:")
      df.printSchema()

      println("\nData:")
      df.show(20, truncate = false)

      println(s"\n✓ Total rows: ${df.count()}")
    } catch {
      case e: Exception =>
        println(s"✗ Spark JDBC Reader failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
