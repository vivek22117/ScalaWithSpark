import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object AirportLatSolution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("airportData2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val data = sc.textFile("in/airports.text")
    val airtportDataForLatitude = data.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val solutionData = airtportDataForLatitude.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + "," + splits(6)
    })

    solutionData.take(10) foreach println
  }
}
