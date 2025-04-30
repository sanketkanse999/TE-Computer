import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine

object SparkCalculator {
  def main(args: Array[String]): Unit = {

    // Initialize the SparkSession
    val spark = SparkSession.builder()
      .appName("Simple Calculator with Spark")
      .master("local[4]") // runs locally with 4 cores
      .getOrCreate()

    // Function to perform basic calculations
    def calculate(num1: Double, num2: Double, operation: String): Double = {
      operation match {
        case "add" => num1 + num2
        case "subtract" => num1 - num2
        case "multiply" => num1 * num2
        case "divide" =>
          if (num2 != 0) num1 / num2
          else {
            println("Error: Division by zero.")
            Double.NaN
          }
        case _ =>
          println("Invalid operation.")
          Double.NaN
      }
    }

    // Take user input for numbers and operation
    println("Enter the first number:")
    val num1 = readLine().toDouble

    println("Enter the second number:")
    val num2 = readLine().toDouble

    println("Enter the operation (add, subtract, multiply, divide):")
    val operation = readLine().trim.toLowerCase

    // Create an RDD with input values and the operation
    val inputRDD = spark.sparkContext.parallelize(List((num1, num2, operation)))

    // Perform the calculation using Spark's map transformation
    val resultRDD = inputRDD.map { case (n1, n2, op) =>
      val result = calculate(n1, n2, op)
      result
    }

    // Collect the result and print it
    val result = resultRDD.collect().head
    println(s"Result: $result")

    // Stop the Spark session
    spark.stop()
  }
}
