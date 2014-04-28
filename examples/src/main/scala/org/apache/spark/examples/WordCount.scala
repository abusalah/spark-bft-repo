/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** Word Count */
object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCount <master> <file>")
      System.exit(1)
    }
    println("0: "+args(0)+" 1: "+args(1))
    val spark = new SparkContext(args(0), "WordCount",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    println("HI DEREK!")
    val file = spark.textFile(args(1), 1)
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    // val fmap = file.flatMap(line => line.split(" "))
    // val m = fmap.map(word => (word, 1))
    // val bkey = m.reduceByKey(_+_)
    // for ((k,v) <- counts) {
    //   println("("+k+", "+v+")")
    // }
    // println("*******FLAT MAP: *********")
    // for (v <- fmap) {
    //   println("("+v+")")
    // }
    // println("*******MAP: *********")
    // for (v <- m) {
    //   println("("+v+")")
    // }
    // println("*******BY KEY: *********")
    // for (v <- bkey) {
    //   println("("+v+")")
    // }
    for (c <- counts) {
      println(c)
    }

    spark.stop()
  }
}
