package com.junjie.api

import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Window测试，watermark处理乱序数据
 *
 * @author chujunjie
 * @date Create in 14:52 2020/9/19
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置事件时间处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputPath = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream
      .map(data => {
        val splits = data.split(",")
        SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 升序数据直接提取时间戳
      .assignTimestampsAndWatermarks((context: WatermarkGeneratorSupplier.Context) => {
        new WatermarkGenerator[SensorReading] {

          // 当前最大时间戳
          var maxTimestamp: Long = _
          // 当前延迟
          var delay: Long = 3000

          override def onEvent(event: SensorReading, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            maxTimestamp = maxTimestamp.max(event.timestamp)
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            output.emitWatermark(new Watermark(maxTimestamp - delay))
          }
        }
      })

    dataStream.keyBy(_.id)
      .timeWindow(Time.seconds(15))
      .reduce((currRes, newRes) =>
        SensorReading(currRes.id, newRes.timestamp, currRes.temperature.min(newRes.temperature))
      )
      .print()

    env.execute("Window Test")
  }
}
