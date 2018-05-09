package sparkSQL

import org.apache.spark.sql.SparkSession

/**
  * Created by Vivek Kumar Mishra on 01-05-2018.
  */
object FakeFriendsInSQL {

  case class Person(Id: Int, name: String, age: Int, numOfFriends: Int)

  def mapper(line: String): Person = {
    val fields = line.split(",")

    val person: Person = Person(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt)

    return person
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Friends").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val lines = spark.sparkContext.textFile("in/fakefriends.csv")
    val data = lines.map(mapper)

    import spark.implicits._
    val schemaPeople = data.toDS

    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age > 12 AND age < 20")

    val result = teenagers.collect()

    result.foreach(println)

    spark.stop()
  }

}
