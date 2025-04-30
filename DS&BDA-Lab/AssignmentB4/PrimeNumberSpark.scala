import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readInt

object PrimeNumberSpark {

  def main(args: Array[String]): Unit = {
    // Initialize the Spark session
    val spark = SparkSession.builder()
      .appName("Prime Number Checker with Spark")
      .master("local[4]")  // Running locally with 4 cores
      .getOrCreate()

    // Define the prime checking function outside the object
    def isPrime(n: Int): Boolean = {
      if (n <= 1) return false
      for (i <- 2 to math.sqrt(n).toInt) {
        if (n % i == 0) return false
      }
      true
    }

    // Prompt the user for input
    println("Enter a number to check if it's prime:")
    val number = readInt()

    // Create an RDD containing the single input number
    val numberRDD = spark.sparkContext.parallelize(List(number))

    // Check if the number is prime
    val resultRDD = numberRDD.map(num => {
      if (isPrime(num)) {
        (num, "Prime")
      } else {
        (num, "Not Prime")
      }
    })

    // Collect and display the result
    resultRDD.collect().foreach {
      case (num, status) => println(s"The number $num is $status.")
    }

    // Stop the Spark session
    spark.stop()
  }
}
