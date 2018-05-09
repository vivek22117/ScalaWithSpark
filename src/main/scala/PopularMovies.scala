import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object PopularMovies {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PopularMovies").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val data = sc.textFile("in/ratings.csv").filter(line => !line.contains("movieId"))
    val moviesMap = data.map(line => (line.split(",")(1).toInt, 1))

    val moviesOccurences = moviesMap.reduceByKey((x, y) => x + y)
    val mapOccurenceToMovies = moviesOccurences.map(a => (a._2, a._1))

    val sortOccurences = mapOccurenceToMovies.sortByKey()
    val result = sortOccurences.collect()

    result.foreach(println)
  }
}
