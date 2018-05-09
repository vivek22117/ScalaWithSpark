import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object AirportSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("airports")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val data = sc.textFile("in/airports.text")
    val airportData = data.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

    val airportNameAndCity = airportData.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + "," + splits(2)
    })

    airportNameAndCity.take(50) foreach println
//    airportNameAndCity.saveAsTextFile("out/airportsSolution.text")
  }

}
