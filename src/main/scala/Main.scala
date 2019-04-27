import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("project_3")
      .master("local")
      .getOrCreate()

    val sc = session.sparkContext
    sc.setLogLevel("OFF")

    val helper = new EdgesHelper(session)

    //    val trainData = helper.loadEdges(args(0))

    val graph = helper.loadGraph(args(0))

    graph.edges.take(100).foreach(println(_)) //JUST CHECKING THAT READING IS OK!


    //    println(trainData.count())
    //    trainData.take(100).foreach(println)

  }
}
