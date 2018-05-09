import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object PrimeNumSum {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sum").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val data = sc.textFile("in/prime_nums.text")

    val numbers = data.flatMap(line => line.split("\\s+"))
    val validNumbers = numbers.filter(number => !number.isEmpty())
    val intNumbers = validNumbers.map(num => num.toInt)

    val primeSum = intNumbers.reduce((x, y) => x + y)



    println("SUm is: " + intNumbers.reduce((x,y) => x + y))

    numbers.take(20) foreach println
    validNumbers.take(20) foreach println
  }
}
