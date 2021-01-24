package com.junjie.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Random

/**
 * Source算子测试
 *
 * @author chujunjie
 * @date Create in 14:52 2020/9/6
 */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.从集合中读取
    val dataList = List(
      SensorReading("sensor_1", 1599375403, 35.8),
      SensorReading("sensor_2", 1599375404, 15.4),
      SensorReading("sensor_3", 1599375405, 6.7),
      SensorReading("sensor_4", 1599375406, 38.1),
      SensorReading("sensor_5", 1599375407, 24.3)
    )
    val streamFromCollection = env.fromCollection(dataList)
    streamFromCollection.print()

    // 2.从文件中读取
    val inputPath = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    val streamFromText = env.readTextFile(inputPath)
    streamFromText.print()

    // 3.从Kafka中读取
    val properties = new Properties()
    // 使用ClassLoader加载properties配置文件生成对应的输入流
    val in = this.getClass.getClassLoader.getResourceAsStream("config/kafka.properties")
    // 使用properties对象加载输入流
    properties.load(in)
    val streamFromKafka = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
    streamFromKafka.print()

    // 4.自定义Source
    val streamFromCustom = env.addSource(new MySensorSourceFunction)
    streamFromCustom.print()

    env.execute("Source Test")
  }
}

/**
 * 自定义sourceFunction
 */
class MySensorSourceFunction() extends SourceFunction[SensorReading] {

  // 标志位，标识数据源是否正常发出数据
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 生成随机数据集
    val random = Random
    var currTemp = 1.to(10).map(i => ("sensor_" + i, random.nextDouble() * 100))

    while (running) {
      currTemp = currTemp.map(data => (data._1, data._2 + random.nextGaussian()))
      val timeStamp = System.currentTimeMillis()
      currTemp.foreach(data => sourceContext.collect(SensorReading(data._1, timeStamp, data._2)))

      Thread.sleep(10000)
    }
  }

  override def cancel(): Unit = running = false
}
