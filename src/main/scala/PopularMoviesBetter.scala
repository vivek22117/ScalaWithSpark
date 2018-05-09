import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Vivek Kumar Mishra on 30-04-2018.
  */
object PopularMoviesBetter {

  //Method to create map of Movie Id and Movie Name
  def loadMovies(): Map[Int, String] = {

    var moviesMap: Map[Int, String] = Map()

    val dataLines = Source.fromFile("in/movies.csv").getLines().filter(line => !line.contains("movieId"))

//    dataLines.take(5) foreach println

    for (line <- dataLines) {
      var fileds = line.split(",")
      if (fileds.length > 1) {
        moviesMap += (fileds(0).toInt -> fileds(1))
      }
    }
    return moviesMap
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Movies").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//  create broadcast varible for movied id and name
    val nameDict = sc.broadcast(loadMovies)
//    println(nameDict.value)

    val dataFile = sc.textFile("in/ratings.csv").filter(line => !line.contains("movieId"))

    // create Map of Movie Id and 1 then count all ones for each movie
    val moviesOccurences = dataFile.map(line => (line.split(",")(1).toInt, 1)).reduceByKey((a, b) => a + b)

    val mapOccurencesToMovies = moviesOccurences.map(x => (x._2, x._1)).sortByKey()

    //get the movie name from the broadcast variable
    val sortedMoviesWithNames = mapOccurencesToMovies.map(x => (nameDict.value(x._2),x._1))

    val results = sortedMoviesWithNames.collect()
    results.foreach(println)
  }


}
