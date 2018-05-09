import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object FriendsSolution {

  def parseLine(line: String) = {
    val data = line.toString.split(",")
    val age = data(2).toInt
    val friends = data(3).toInt
    (age, friends)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Friends").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val data = sc.textFile("friends.csv").filter(line => !line.contains("Age"))

    val mapData = data.map(parseLine)

    mapData.take(15) foreach println

    val totalOfFriendByAge = mapData.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + x._1, y._2 + y._2))
    val avgOfAge = totalOfFriendByAge.mapValues(x => x._1 / x._2)

    val results = avgOfAge.collect()

    results.sorted.foreach(println)

//    totalOfFriendByAge.take(10) foreach println
  }

}
