package sparkSQL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object LogDataSolution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("logData").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val julyData = sc.textFile("in/nasa_19950701.tsv")
    val augustData = sc.textFile("in/nasa_19950801.tsv")

    val julyHosts = julyData.map(line => line.split("\t")(0))
    val augustHosts = augustData.map(line => line.split("\t")(0))

//    julyHosts.take(20) foreach println
//    augustHosts.take(20) foreach println

    val hostsIntersection = julyHosts.intersection(augustHosts)

    val hostIntersectionWithoutHeaders = hostsIntersection.filter(host => host != "host")

    hostIntersectionWithoutHeaders.take(10) foreach println
  }

}
