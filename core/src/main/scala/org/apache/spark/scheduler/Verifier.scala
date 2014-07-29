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

package org.apache.spark.scheduler

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.util.control._

import org.apache.hadoop.yarn.client.api.Byzantine

/**
 */
private[spark]
class Verifier() extends Logging {

  var verified = true
  private var createErrors = 0
  private val byzantine = new Byzantine
  
  def verify(resultArray: ArrayBuffer[Any]) {
    val loop0 = new Breaks
    val loop1 = new Breaks
    var count = 0
    var votes:Array[Int] = new Array[Int](4)
    // logInfo("RESULTS STRING: "+resultArray(3).toString)
    // logInfo("RESULTS CLASS: "+resultArray(3).getClass)

    resultArray(3) match {
      case Some(_) =>
	// logInfo("Some(int)")

	// for (v <- resultArray) {
	//   logInfo("V: "+v)
	// }
	for (v0 <- 0 until (resultArray.length/2)) {
	  for (v1 <- v0 until resultArray.length) {
	    // logInfo("v0: ["+v0+"] v1: ["+v1+"]")
	    if (resultArray(v0) != resultArray(v1)) {
	      votes(v0) += 1
	      votes(v1) += 1
	    }
	  }
	}

      case a: Int if a >= 0 =>
	// logInfo("if a > 0")

	// for (v <- resultArray) {
	//   logInfo("V: "+v)
	// }
	for (v0 <- 0 until (resultArray.length/2)) {
	  for (v1 <- v0 until resultArray.length) {
	    // logInfo("v0: ["+v0+"] v1: ["+v1+"]")
	    if (resultArray(v0) != resultArray(v1)) {
	      votes(v0) += 1
	      votes(v1) += 1
	    }
	  }
	}

      case _ =>
	logInfo("_")
	// this matches array of Tuple2's
	resultArray(0).asInstanceOf[Array[Tuple2[_, _]]](0) match {
	  case (_, _) =>
	    logInfo("(_, _)")
	      
	    var tuples = new ArrayBuffer[Array[Tuple2[_, _]]]
	    for (i <- 0 until resultArray.length) {
	      tuples += resultArray(i).asInstanceOf[Array[Tuple2[_, _]]]
	    }
	    
	    val length = tuples(0).length
	    for (i <- 1 until tuples.length) {
	      if (tuples(i).length != length) {
		verified = false
		return 
	      }
	    }
	    loop0.breakable {
	      for (i <- 0 until tuples.length) {
		for (j <- i+1 until tuples.length) {
		  logInfo("i: "+i+" j: "+j)
		  loop1.breakable {
		    for (k <- 0 until tuples(i).length) {
		      logInfo("Comparing: "+tuples(i)(k)._1+ " : "+tuples(j)(k)._1+" , "+tuples(i)(k)._2+" : "+tuples(j)(k)._2)
		      if (!tuples(i)(k)._1.equals(tuples(j)(k)._1) || tuples(i)(k)._2 != tuples(j)(k)._2) {
			logInfo("Setting votes("+i+")="+(votes(i)+1)+" votes("+j+")="+(votes(j)+1))
			votes(i) += 1
			votes(j) += 1
			loop1.break
		      }
		    }
		  }
		}
		if (votes(i) == 0) {
		  loop0.break
		}
		if (votes(i) < 2) {
		  votes(i) = 0
		}
		else {
		  votes(i) = 1
		}
	      }
	    }
	  case _ =>
	    logInfo("result(0)(0): _")
	}
    }
    var broke = false
    loop0.breakable {
      for (v <- 0 until (votes.length/2)) {
	if (votes(v) < byzantine.getNumReplicas/2) {
	  broke = true
	  loop0.break
	}
      }
    }
    if (!broke) {
      logError("THESE CONTAINERS AREN'T OKAY!!!")
      verified = false
    }
    else {
      logInfo("THESE CONTAINERS ARE GREAT!!!!!!!!!!!!!--------------------")
    }
  }
}
