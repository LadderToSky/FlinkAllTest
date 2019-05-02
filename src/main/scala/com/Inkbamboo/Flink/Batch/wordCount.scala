/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.Inkbamboo.Flink.Batch

import org.apache.flink.api.scala._

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
object wordCount {

  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
/*    val dataset = env.readTextFile("E:\\test.txt")
   // env.readCsvFile("E:\\testfile")
   val grouped =  dataset.flatMap(x=>x.toLowerCase.split("\\w+").filter(x=>x.nonEmpty))
        .map(x=>(x,1))
        .groupBy(0)   //根据第一个字段进行分组汇聚
        grouped.sum(1)    //对分组后的数据，根据指定字段进行指定操作
      .writeAsCsv("E:\\testfile\\","\n"," ")*/

    case class word(word:String,count:Int)
    val dataset = env.fromElements(word("helloword",1),word("Inkbamboo",2),
      word("helloword",1),word("Inkbamboo",2),
      word("helloword",3),word("Inkbamboo",2))

    dataset.groupBy("word").sum(1).print()
    println("--------One------")
   dataset.groupBy(0, 1).sum(1).print()

env.execute("testfile")
    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataSet[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/batch/index.html
     *
     * and the examples
     *
     * http://flink.apache.org/docs/latest/apis/batch/examples.html
     *
     */

    // execute program
    //env.execute("Flink Batch Scala API Skeleton")
  }
}
