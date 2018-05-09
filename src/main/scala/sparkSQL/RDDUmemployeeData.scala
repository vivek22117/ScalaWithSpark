package sparkSQL

import org.apache.spark.{SparkConf, SparkContext}

import scalafx.application.JFXApp

/**
  * Created by Vivek Kumar Mishra on 22-04-2018.
  */

case class Area(code: String, text: String)

case class Series(id: String, code: String, measure: String, title: String)

case class LAData(id: String, year: Int, period: Int, value: Double)

object RDDUmemployeeData extends JFXApp {

  val conf = new SparkConf().setAppName("Emp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val areas = sc.textFile("la.area.txt").filter(!_.contains("area_code")).map { line =>
    val p = line.split("\t").map(_.trim)
    Area(p(1), p(2))
  }.cache()

  //  areas.take(10) foreach println

  val series = sc.textFile("la.series.txt").filter(!_.contains("area_code")).map { line =>
    val s = line.split("\t").map(_.trim)
    Series(s(0), s(2), s(3), s(6))
  }.cache()

  //  series.take(10) foreach println

  val laData = sc.textFile("la.data.30.Minnesota.txt").filter(!_.contains("year")).map { line =>
    val la = line.split("\t").map(_.trim)
    LAData(la(0), la(1).toInt, la(2).drop(1).toInt, la(3).toDouble)
  }.cache()

  //  laData.take(10) foreach println


  val rates = laData.filter(_.id.endsWith("3"))
  val decadeGroups = rates.map(d => (d.id, d.year / 10) -> d.value)
  val decadeAvg = decadeGroups.aggregateByKey(0.0 -> 0)({ case ((s, c), d) => (s + d, c + 1) }, { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }).mapValues(x => x._1 / x._2)


  val mapDecade = decadeAvg.map(x => (x._1._1, (x._1._2, x._2)))
    .reduceByKey { case ((x1, y1), (x2, y2)) => if (y1 >= y2) (x1, y1) else (x2, y2) }

  val seriesData = series.map(x => (x.id, x.title))

  val joinedMaxDecades = seriesData.join(mapDecade)

  joinedMaxDecades.take(10) foreach println

  decadeAvg.take(10) foreach println
  mapDecade.take(5) foreach println


  sc.stop()
}
