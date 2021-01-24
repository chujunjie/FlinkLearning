package com.junjie.api

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * 状态编程
 *
 * @author chujunjie
 * @date Create in 12:37 2020/9/20
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 状态后端，设置状态checkpoint
    env.setStateBackend(new FsStateBackend(""))
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60 * 1000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val alertStream = dataStream
      .keyBy(_.id)
      // 同一采集器相邻温差超过10.0则报警
      //      .flatMap(new TempChangeAlert(10.0))
      .flatMapWithState[(String, Double, Double), Double]({
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp: Some[Double]) =>
          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
          } else {
            (List.empty, Some(data.temperature))
          }
      })

    alertStream.print()

    env.execute("State Test")
  }
}

class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  // 保存上一次的温度值
  lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = valueState.value()
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    valueState.update(value.temperature)

  }
}

class MyRichMapper extends RichMapFunction[SensorReading, String] {

  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] =
    getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))

  override def open(parameters: Configuration): Unit = {
    // 通过运行时上下文获取状态句柄
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    // 状态读写
    val stateValue = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    value.id
  }
}
