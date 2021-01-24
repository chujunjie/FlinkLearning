package com.junjie.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 流处理
 *
 * @author chujunjie
 * @date Create in 13:33 2020/9/5
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    // 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 全局独占Slot
//    env.disableOperatorChaining()

    // 从入参提取参数
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    // 接收socket文本流
    //    val input = env.socketTextStream("localhost", 7777)
    val input = env.socketTextStream(host, port).slotSharingGroup("a")

    // 使用disableChaining()/startNewChain()等自定义任务调度规则
    val res = input
      .flatMap(_.split(" ")).slotSharingGroup("b")
      .filter(_.nonEmpty).slotSharingGroup("a")
      .map((_, 1))
      .keyBy(a => (a._1, a._2))
      .sum(1)

    res.print().setParallelism(1)
    env.execute("Stream WordCount")
  }
}
