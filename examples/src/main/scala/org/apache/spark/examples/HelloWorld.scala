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

/**  */
object HelloWorld {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: HelloWorld <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "HelloWorld",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val slices = if (args.length > 1) args(1).toInt else 2

    val n = 100 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      i
    }.reduce(_ + _)

    println("Sum is " + count)
    spark.stop()
  }
}
