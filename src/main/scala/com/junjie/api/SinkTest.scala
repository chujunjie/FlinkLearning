package com.junjie.api

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * sink测试
 *
 * @author chujunjie
 * @date Create in 22:00 2020/9/14
 */
object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    val stream = env.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val sensor = data.split(",")
      SensorReading(sensor(0), sensor(1).toLong, sensor(2).toDouble)
    })

    // 1.file sink
    val path = this.getClass.getClassLoader.getResource("").getPath
    dataStream.addSink(StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder[SensorReading]()).build())

    // 2.kafka sink
    val stringStream = dataStream.map(data => {
      data.toString
    })
    val kafkaSink = new FlinkKafkaProducer[String]("", "", new SimpleStringSchema())
    stringStream.addSink(kafkaSink)

    // 3.redis sink
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()
    dataStream.addSink(new RedisSink[SensorReading](config, new MyRedisMapper))

    // 4.es sink
    val httpHosts = util.Arrays.asList(new HttpHost("localHost", 9200))
    val esSink = new ElasticsearchSink.Builder[SensorReading](httpHosts, (data, b, requestIndexer) => {
      val dataSource = new util.HashMap[String, String]()
      dataSource.put("id", data.id)
      dataSource.put("temperature", data.temperature.toString)
      dataSource.put("timestamp", data.timestamp.toString)

      val request = Requests.indexRequest()
        .index("sensor")
        .`type`("readingdata")
        .source(dataSource)

      requestIndexer.add(request)
    }).build()
    dataStream.addSink(esSink)

    // 5.mysql sink
    dataStream.addSink(new MyJdbcSink)

    env.execute("File Sink Test")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {

  // 定义保存数据写入redis的命令，HSet 表名 key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  override def getValueFromData(t: SensorReading): String = t.id
}

class MyJdbcSink extends RichSinkFunction[SensorReading] {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 定义连接、预编译语句
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  // 关闭连接
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
}
