import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Graph, VertexId}
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
  }

  def loadGraph(path: String): Graph[VertexId, Int] = {

    // TODO: Как же тебя собака сразу в чтение засунуть, чтобы ты лонгом была, а не инт?
    val edgesRDD: RDD[(VertexId, VertexId)] = loadEdges(path).map { case (a, b) => (a.toLong, b.toLong) }

    // TODO: Чё за default value? По идее можно передавать сюда пару с индексом и присваивать этому value значения.
    Graph.fromEdgeTuples(edgesRDD, 1)
  }

}
