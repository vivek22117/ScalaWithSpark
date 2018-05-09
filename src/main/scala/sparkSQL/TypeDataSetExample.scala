package sparkSQL

import org.apache.spark.sql.{Encoders, SparkSession}

import scalafx.application.JFXApp

/**
  * Created by Vivek Kumar Mishra on 22-04-2018.
  */

//case class Series(id:String,area:String,measure:String,title:String)
case class LACountry(id:String,year:Int,period:String,value:Double)

object TypeDataSetExample extends JFXApp {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  val countryData = spark.read.schema(Encoders.product[LACountry].schema).option("headers",true)
    .option("delimiter","\t").csv("la-country-64")

}
