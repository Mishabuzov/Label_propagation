import java.io.File

import breeze.linalg.csvwrite
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

    val trainData = helper.loadEdges(args(0))

    // JUST CHECKING THAT READING IS OK!
    // REmove after done.
    println(trainData.count())
    trainData.take(100).foreach(println)

    //    csvwrite(new File("emb_in.csv"), embIn, separator = ',')

  }
}
