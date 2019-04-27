import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class EdgesHelper(session: SparkSession) {

  /* Return RDD of tuple (int, int) for each edge in the input file. */
  def loadEdges(path: String): RDD[(Int, Int)] = {
    session.read.format("csv")
      // header: source_node, destination_node
      // here we read the data from TXT and export it as RDD[(Int, Int)],
      // i.e. as RDD of edges
      .option("header", "true")
      // State that the header is present in the file
      .schema(StructType(Array(
      StructField("source_node", IntegerType, false),
      StructField("destination", IntegerType, false)
    )))
      // Define schema of the input data
      .load(path)
      // Read the file as DataFrame
      .rdd.map(row => (row.getAs[Int](0), row.getAs[Int](1)))
    // Interpret DF as RDD
  }

}
