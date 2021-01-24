package com.junjie.api

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * Process Function
 *
 * @author chujunjie
 * @date Create in 14:04 2020/9/20
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 1.温度连续上升报警
    val warningStream = dataStream
      .keyBy(_.id)
      .process(new TempRiseWarning(10 * 1000L))

    warningStream.print()

    // 2.高低温侧输出流
    val highTempStream = dataStream.process(new SplitTempProcessor(30.0))
    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[SensorReading]("low")).print("low")

    env.execute("Process Function Test")
  }
}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    // 大于阈值输出到主流，否则输出到侧输出流
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[SensorReading]("low"), value)
    }
  }
}

class TempRiseWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {

  // 保存上一个温度
  lazy val valueState: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

  // 保存定时器的时间戳用于删除
  lazy val timerTsState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTsState", classOf[Long]))

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {

    val lastTemp = valueState.value()
    val timerTs = timerTsState.value()

    // 温度上升且当前没有定时器
    if (value.temperature > lastTemp && lastTemp == 0) {
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      timerTsState.update(ts)
    } else if (value.temperature < lastTemp) {
      // 温度下降，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear()
    }

    valueState.update(value.temperature)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval / 1000 + "秒上升")
    timerTsState.clear()
  }
}
