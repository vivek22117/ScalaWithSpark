import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


/*import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import swiftvis2.spark._*/

import scalafx.application.JFXApp

/**
  * Created by Vivek Kumar Mishra on 04-03-2018.
  */
object SparkSQLExample extends JFXApp {

  val spark = SparkSession.builder().master("local[*]").appName("NOADat").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val tSchema = StructType(Array(StructField("sId", StringType),
    StructField("date", DateType),
    StructField("mType", StringType),
    StructField("value", DoubleType)))

  val sSchema = StructType(Array(StructField("id", StringType),
    StructField("lat", DoubleType),
    StructField("lon",DoubleType),
    StructField("name",StringType)))

  val stationsRDD= spark.sparkContext.textFile("ghcnd-stations.txt").map{line =>
    val id = line.substring(0,11)
    val lat = line.substring(12,20).toDouble
    val lon = line.substring(21,30).toDouble
    val name = line.substring(41,71)
    Row(id,lat,lon,name)
  }

  val stations = spark.createDataFrame(stationsRDD,sSchema).cache()
  val temData2017 = spark.read.schema(tSchema).option("dateFormat", "yyyyMMdd").csv("2017.csv")

  temData2017.createOrReplaceTempView("tempData2017")

  val pureSQL = spark.sql(
    """
       SELECT * FROM
       (SELECT sId, date, value as tMax FROM tempData2017 WHERE mType = "TMAX" LIMIT 1000)
       JOIN
       (SELECT sId, date, value as tMin FROM tempData2017 WHERE mType = "TMIN" LIMIT 1000)
       USING (sId, date)
    """)
  pureSQL.show()

  /*val maxTemp2017 = temData2017.filter($"mType" === "TMAX").drop("mType").withColumnRenamed("value", "maxTemp")
  val minTemp2017 = temData2017.filter('mType === "TMIN").drop("mType").withColumnRenamed("value", "minTemp")

  maxTemp2017.show()
  minTemp2017.show()

  val combinedTempData = maxTemp2017.join(minTemp2017, Seq("sId", "date"))
  val avgCombinedTempData = combinedTempData.select('sId, 'date, ('maxTemp + 'minTemp) / 20 * 1.8 + 32)
    .withColumnRenamed("((((maxTemp + minTemp) / 20) * 1.8) + 32)", "avgTemp")

  val groupByStationId = avgCombinedTempData.groupBy("sId").agg(avg("avgTemp"))

  groupByStationId.show()

  val joinedTempDataAndStations = groupByStationId.join(stations,"sId")
  joinedTempDataAndStations.show()*/
//  avgCombinedTempData.show()
  //  combinedTempData.show()


  /*val tSchema = StructType(Array(StructField("sId", StringType),
    StructField("date", DateType),
    StructField("mType", StringType),
    StructField("value", DoubleType)))

  val data2017 = spark.read.schema(tSchema).option("dateFormat", "yyyyMMdd").csv("2017.csv")
  data2017.show()
  data2017.schema.printTreeString()

  val tMax = data2017.filter($"mType" === "TMAX").drop("mType").withColumnRenamed("value", "maxTemp")
  val tMin = data2017.filter('mType === "TMIN").drop("mType").withColumnRenamed("value", "minTemp")
  //  tMin.show()
  //  tMax.show()

  //  val combinedData = tMax.join(tMax, tMax("sId") === tMin("sId") && tMax("date") === tMin("date"))
  val comibnedData = tMax.join(tMin, Seq("sId", "date"))
  val avgTempData = comibnedData.select('sId, 'date, ('maxTemp + 'minTemp)/ 20 * 1.8 + 32)
                                    .withColumnRenamed("((minTemp + maxTemp) / 20 * 1.8 + 32)", "avgTemp")
  //  comibnedData.show()
  avgTempData.show()
*/
  spark.stop()

}
