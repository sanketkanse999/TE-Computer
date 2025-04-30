import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine

object NumberCheck {
  def main(args: Array[String]): Unit = {

    // Initialize the SparkSession
    val spark = SparkSession.builder()
      .appName("Number Check with Spark")
      .master("local[4]") // runs locally with 4 cores
      .getOrCreate()

    // List to hold user inputs
    var numberList = List[Int]()

    // Take user input in a loop until the user decides to stop
    var input = ""
    while (input != "exit") {
      println("Enter a number (or type 'exit' to stop):")
      input = readLine().trim

      if (input != "exit") {
        try {
          // Convert input to an integer and add it to the list
          val number = input.toInt
          numberList = numberList :+ number
        } catch {
          case e: NumberFormatException => println("Please enter a valid number.")
        }
      }
    }

    // Check if the list is not empty
    if (numberList.nonEmpty) {
      // Create an RDD from the user input list
      val inputRDD = spark.sparkContext.parallelize(numberList)

      // Process the input using Spark's map function
      val resultRDD = inputRDD.map(num => {
        if (num > 0) {
          (num, "positive")
        } else if (num < 0) {
          (num, "negative")
        } else {
          (num, "zero")
        }
      })

      // Collect and display the result
      resultRDD.collect().foreach {
        case (num, status) => println(s"The number $num is $status.")
      }
    } else {
      println("No numbers were entered.")
    }

    // Stop the Spark session
    spark.stop()
  }
}
