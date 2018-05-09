import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val dataFile = sc.textFile("wordCountDoc.txt")

    val words = dataFile.flatMap(line => line.split(" "))
    val wordCounts = words.countByValue()

    for ((word, count) <- wordCounts) println(word + " : " + count)
    wordCounts.take(100) foreach print
  }
}
