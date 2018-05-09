import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vivek Kumar Mishra on 29-04-2018.
  */
object WordCountBetter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Better").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val data = sc.textFile("in/word_count.text")
    val words = data.flatMap(line => line.split("\\W+"))
    val mapOfWords = words.map(x => x.toLowerCase()).map(y => (y, 1)).reduceByKey((a, b) => a + b)

//    mapOfWords.foreach(println)

    val wordCountSorted = mapOfWords.map(x => (x._2, x._1)).sortByKey()

    for(result <- wordCountSorted){
      val count = result._1
      val word = result._2
      println(s"$word:  $count")
    }

    wordCountSorted.foreach(println)
  }
}
