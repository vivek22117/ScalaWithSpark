import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HARSHA on 25-02-2018.
  */

case class TempData(day: Int, doy: Int, month: Int, year: Int, precp: Double,
                    snow: Double, tavg: Double, tmax: Double, tmin: Double)

object RDDTempData {
  def toDouble(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _: NumberFormatException => -1
    }
  }

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("RDD App").setMaster("local[*]")
    var sc = new SparkContext(conf)

    val lines = sc.textFile("MMDATAFile1.csv").filter(!_.contains("Day"))

    val data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, toDouble(p(5))
          , toDouble(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }.cache()
//    data.take(5) foreach println

    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((t1,t2) => if(t1.tmax > t2.tmax) t1 else t2))

    val rainyCount = data.filter(_.precp >= 1.0).count()
    println(s"There is $rainyCount rainly days. There is ${rainyCount * 100.00/data.count()} percent")

    val (rainySum, rainyCnt) = data.aggregate(0.0 -> 0)({case ((sum,cnt), td) =>
      if(td.precp < 1.0) (sum,cnt) else (sum+td.tmax,cnt+1)},{case ((s1,c1),(s2,c2))=>(s1+s2,c1+c2)})

    println(s"Average rainy temp is ${rainySum/rainyCnt}")

    val rainyTemps = data.flatMap(td => if(td.precp < 1.0) Seq.empty else Seq(td.tmax))
    println(s"Average rainy temp using flatmap is ${rainyTemps.sum/rainyTemps.count}")

    val monthsGroup = data.groupBy(_.month)
    val monthlyTemp = monthsGroup.map{case (m, days) => m -> days.foldLeft(0.0)((sum,td)=> sum+td.tmax)/days.size}

    monthlyTemp.collect.sortBy(_._1) foreach println
  }

}
