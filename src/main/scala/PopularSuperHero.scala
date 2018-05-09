import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 01-05-2018.
  */
object PopularSuperHero {

  //function to extract hero Id and no of connections
  def countFriends(line: String) = {
    var fields = line.split("\\s+")
    (fields(0).toInt, fields.length - 1)
  }

  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Heros").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    //build up HeroId -> Name
    val data = sc.textFile("in/Marvel-Names.txt")
    val heroIdName = data.flatMap(parseNames)

    heroIdName.take(20) foreach println

    //convert hero Id and no of connections
    val data2 = sc.textFile("in/Marvel-Graph.txt")
    val heroAndConnections = data2.map(countFriends).reduceByKey((a, b) => a + b)

    val connectionAndHero = heroAndConnections.map(x => (x._2,x._1))

    val mostConnectedHero = connectionAndHero.max()

    println(mostConnectedHero)

    //lookUp return array so we have to use (0) to fetch the first element
    val mostPopularName = heroIdName.lookup(mostConnectedHero._2)(0)

    println(s"$mostPopularName is the most popular Super Hero with ${mostConnectedHero._1} co-apperances")
  }

}
