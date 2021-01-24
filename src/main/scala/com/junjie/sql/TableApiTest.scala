package com.junjie.sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * Table Api
 *
 * @author chujunjie
 * @date Create in 14:54 2020/9/21
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.1基于old planner的流处理
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2基于blink planner的流处理
    val blinkStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 2.连接外部系统，读取数据注册表
    // 2.1读取文件
    val filePath = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("input_sensor_table")

    val table = tableEnv.from("input_sensor_table")
    //    table.toAppendStream[(String, Long, Double)].print()

    // 2.2从kafka读取
    //    tableEnv
    //      .connect(new Kafka()
    //        .version("0.11")
    //        .topic("sensor")
    //        .property("zookeeper.connect", "localhost:2181")
    //        .property("bootstrap.servers", "localhost:9092"))
    //      .withFormat(new Csv)
    //      .withSchema(new Schema()
    //        .field("id", DataTypes.STRING())
    //        .field("timestamp", DataTypes.BIGINT())
    //        .field("temperature", DataTypes.DOUBLE()))
    //      .createTemporaryTable("input_kafka_table")
    //
    //    val kafkaTable = tableEnv.from("input_kafka_table")
    //    kafkaTable.toAppendStream[(String, Long, Double)].print()

    // 3.查询转换
    // 3.1 使用table api
    val sensorTable = tableEnv.from("input_sensor_table")
    val resultTable = sensorTable
      .select("id, temperature")
      .filter("id === 'sensor_1'")

    // 3.2 使用sql api
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from input_sensor_table
        |where id = 'sensor_1'
        |""".stripMargin)
    resultSqlTable.toAppendStream[(String, Double)].print()

    // 3.3 聚合, count
    val aggTable = sensorTable
      .groupBy("id")
      .select("id, id.count as count")

    // 4.输出
    // 4.输出到文件
    val outPath = this.getClass.getClassLoader.getResource("").getPath + "sensor_out.txt"
    tableEnv
      .connect(new FileSystem().path(outPath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("output_sensor_table")

    resultTable.executeInsert("output_sensor_table")

    env.execute("Table Api TesT")
  }
}
