package com.dfitc.wc

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "/home/frank/IdeaProjects/flink-demo/src/main/resources/hello.txt"
    var inputDataSet = env.readTextFile(inputPath)
    val wordCountDataSet = inputDataSet.flatMap (_.split(" "))
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)
    wordCountDataSet.print()
  }
}
