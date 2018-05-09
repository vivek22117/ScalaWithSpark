package sparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Created by Vivek Kumar Mishra on 02-05-2018.
  */
object MovieRatingsSQLAPI {

  def loadMovies(): Map[Int, String] = {
    var movies: Map[Int, String] = Map()

    val data = Source.fromFile("in/movies.csv").getLines().filter(line => !line.contains("movieId"))
    for (line <- data) {
      var fields = line.split(",")
      if (fields.length > 1) {
        movies += (fields(0).toInt -> fields(1))
      }
    }
    return movies
  }

  final case class Movie(movieID: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MovieMap").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val readData = spark.sparkContext.textFile("in/ratings.csv")
      .filter(x => !x.contains("movieId"))
      .map(line => Movie(line.split(",")(1).toInt))

    //    val data = spark.read.format("csv").option("header",true).load("in/ratings.csv")

    import spark.implicits._
    val movieIds = readData.toDS
    val calRatings = movieIds.groupBy("movieID").count().orderBy(desc("count")).cache()

    val topTenMovies = calRatings.take(10)

    val names = loadMovies()

    for (result <- topTenMovies) {
            println(names(result(0).asInstanceOf[Int]) +" : "+ result(1))
    }


    spark.stop()
  }

}
