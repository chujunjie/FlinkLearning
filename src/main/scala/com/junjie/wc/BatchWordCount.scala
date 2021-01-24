package com.junjie.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
 * 批处理
 *
 * @author chujunjie
 * @date Create in 0:59 2020/9/4
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val path: String = this.getClass.getClassLoader.getResource("word.txt").getPath
    val inputDataSet: DataSet[String] = env.readTextFile(path)

    val res: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    res.print()
  }
}
