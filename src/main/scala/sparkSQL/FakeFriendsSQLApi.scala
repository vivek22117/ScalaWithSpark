package sparkSQL

import org.apache.spark.sql.SparkSession

/**
  * Created by Vivek Kumar Mishra on 01-05-2018.
  */
object FakeFriendsSQLApi {

  case  class Person(Id:Int,name:String,age:Int,numOfFriends:Int)

  def mapper(line:String):Person ={
    val fields = line.split(",")
    val person:Person = Person(fields(0).toInt,fields(1).toString,fields(2).toInt,fields(3).toInt)

    return person
  }

  def main(args:Array[String]) ={

    val spark = SparkSession.builder().appName("FriendsSQLAPI").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.sparkContext.textFile("in/fakefriends.csv")
    val data = lines.map(mapper).toDS.cache()

    data.printSchema()
    data.select("name").show()

    data.filter(data("age") > 19).show()

    data.groupBy("age").count().show()
    data.select(data("name"),data("age") + 10 as "newAge").show()

    spark.stop()
  }
}
