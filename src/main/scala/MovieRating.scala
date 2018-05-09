import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object MovieRating {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Rating").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("in/ratings.csv").filter(line => !line.contains("userId"))

    data.take(5) foreach println

    val movieRatingData = data.map(line => line.toString.split(",")(2).toDouble)
    val ratingsMap = movieRatingData.countByValue()
    val finalData = ratingsMap.toSeq.sortBy(_._1)

    finalData.take(100) foreach println
  }

}
