package com.junjie.api

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.Seq

/**
 * Transform算子测试
 *
 * @author chujunjie
 * @date Create in 16:20 2020/9/6
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    val stream = env.readTextFile(inputPath)

    val dataStream = stream.map(data => {
      val sensor = data.split(",")
      SensorReading(sensor(0), sensor(1).toLong, sensor(2).toDouble)
    })

    // 1.分组聚合，输出每个传感器当前最小值
    val aggStream = dataStream.keyBy(x => x.id).minBy("temperature") // min方法其他字段不变
    //    aggStream.print()

    // 2.输出当前时间最小值
    val reduceStream = dataStream
      .keyBy(x => x.id)
      .reduce((curr, newData) => SensorReading(curr.id, newData.timestamp, newData.temperature.min(curr.temperature)))
    //    reduceStream.print()

    // 3.多流转换操作
    // 3.1 分流，将传感器温度分为低温高温两个流
    val splitStream = dataStream.split(x => {
      if (x.temperature > 30.0) Seq("high") else Seq("low")
    })
    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")
    //    highStream.print("high")

    // 3.2 合流connect
    val warningStream = highStream.map(data => (data.id, data.temperature))
    val connectStream = warningStream.connect(lowStream)
    val coMapResultStream = connectStream
      .map(
        warningData => (warningData._1, warningData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy")
      )
    coMapResultStream.print("coMap")

    // 3.3 合流union（类型一致）
    val unionStream = highStream.union(lowStream)
    unionStream.print("union")

    env.execute("TransForm Test")
  }
}
