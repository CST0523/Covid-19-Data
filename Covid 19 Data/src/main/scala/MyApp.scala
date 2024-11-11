import scala.io.Source

// Superclass
// Concrete implementation of DataProcessor trait for processing CSV vaccination data
trait DataProcessor {
  def loadRecords(filename: String): List[Map[String, String]]
  def avgByFields(fieldNames: List[String]): Map[String, Double]
  def maxByField(fieldName: String): (String, Int)
  def countRecordsByType: Map[String, Int]
}

// Subclass
// Class that implements the Data Processor trait for processing CSV vaccination data.
class CsvDataProcessor(filename: String) extends DataProcessor {
  // Load the records from the specified file when the class is instantiated
  private val records: List[Map[String, String]] = loadRecords(filename)

  // Method to load records from file
  def loadRecords(filename: String): List[Map[String, String]] = {
    // Open the file
    val source = Source.fromFile(filename, "UTF-8")
    // Read all lines from the file
    val lines = source.getLines().toList
    // Close file
    source.close()

    // Check file is empty
    if (lines.isEmpty) {
      println("File is empty!")
      List.empty
    } else {
      // Split the first line to get the headers
      val headers = lines.head.split(",").map(_.trim)
      lines.tail.map { line =>
        val values = line.split(",").map(_.trim)
        headers.zip(values).toMap
      }
    }
  }

  // Method to group records by vaccination type
  private def groupByVaxType: Map[String, List[Map[String, String]]] = {
    records.groupBy(_("vaxtype"))
  }

  // Method to calculate average values for given fields across records
  def avgByFields(fieldNames: List[String]): Map[String, Double] = {
    // Group the records by vaccination type
    groupByVaxType.mapValues { records =>
      // Calculate the sum of the specified fields across all records
      val totalValuesSum = records.flatMap { record =>
        fieldNames.flatMap(fieldName => record.get(fieldName).map(_.toDouble))
      }.sum
      // Calculate the average by dividing the sum of values by the number of records
      val recordCount = records.size
      if (recordCount != 0) totalValuesSum / recordCount else 0.0
    }
  }

  // Method to find the record with the maximum value for a given field
  def maxByField(fieldName: String): (String, Int) = {
    // Group the records by vaccination type and sum the specified field for each group
    groupByVaxType.mapValues { records =>
      val values = records.flatMap(_.get(fieldName).map(_.toInt))
      if (values.nonEmpty) values.sum else 0
      // Find the group with the maximum sum
    }.maxBy(_._2)
  }

  // Method to count records by vaccination type
  def countRecordsByType: Map[String, Int] = {
    groupByVaxType.mapValues(_.size)
  }
}

object MyApp {
  def main(args: Array[String]): Unit = {
    val filename = "src/main/resources/aefi.csv"
    val processor: DataProcessor = new CsvDataProcessor(filename)

    // Question 1: Which vaccination product is the most commonly used and how many products were used?
    val recordCountsByType = processor.countRecordsByType
    val mostUsedVaxType = recordCountsByType.maxBy(_._2)
    println("\nQuestion 1:")
    println("Which vaccination product is the most commonly used and how many products were used?")
    println("\nList of all vaccination types: ")
    recordCountsByType.foreach { case (vaxType, count) =>
      println(s"Vaccination type '$vaxType' has $count records.")
    }
    println(s"The most commonly used vaccine is '${mostUsedVaxType._1}' with a total of ${mostUsedVaxType._2} records.")

    // Question 2: What are the average occurrences of headache for each type of vaccination product in the provided data?
    val avgHeadacheByVaxType = processor.avgByFields(List("d1_headache", "d2_headache"))
    println("\nQuestion 2:")
    println("What are the average occurrences of headache for each type of vaccination product in the provided data?")
    avgHeadacheByVaxType.foreach { case (vaxType, avgHeadache) =>
      val formattedAvg = f"$avgHeadache%.2f"
      println(s"Average occurrences of headache for vaccination type '$vaxType' is $formattedAvg")
    }

    // Question 3: Which vaccination type has the highest occurrence of vomiting after first injection in the provided data?
    val maxVomitingVaxType = processor.maxByField("d1_vomiting")
    println("\nQuestion 3:")
    println("Which vaccination type has the highest occurrence of vomiting after first injection in the provided data?")
    println(s"The vaccination type '${maxVomitingVaxType._1}' has the highest occurrence of vomiting after the first injection, totaling ${maxVomitingVaxType._2} times.")
  }
}
