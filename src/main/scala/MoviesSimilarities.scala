import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Vivek Kumar Mishra on 01-05-2018.
  */
object MoviesSimilarities {

  def loadMovies(): Map[Int, String] = {

    var movies: Map[Int, String] = Map()
    val lines = Source.fromFile("in/movies.csv").getLines().filter(line => !line.contains("movieId"))
    for (line <- lines) {
      var fields = line.split(",")
      if (fields.length > 1) {
        movies += (fields(0).toInt -> fields(1))
      }
    }
    return movies
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2
    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    return movie1 > movie2
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MovieRecommend").setMaster("local[3]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val nameDict = loadMovies()
    //    println(nameDict)

    //map rating to key value pair e.g userId => (movieID, rating)
    val data = sc.textFile("in/ratings.csv").filter(line => !line.contains("movieId"))

    val ratingKeyValue = data.map(line => line.split(",")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    //self join to find every combinations
    val joinedRatings = ratingKeyValue.join(ratingKeyValue)

//    joinedRatings.take(20) foreach println

    //filter out duplicate pairs from joinedRatings
    val filteredJoinedRatings = joinedRatings.filter(filterDuplicates)
    //filteredJoinedRatings.take(50) foreach println

    //now create key of (movie1, movie2)
    val movieNamePairs = filteredJoinedRatings.map(makePairs)
//    movieNamePairs.take(50) foreach println

    //now collecting all movie rating pairs for each movie pair
    val movieRatingPairs = movieNamePairs.groupByKey()
    movieRatingPairs.take(5) foreach println

  }
}
