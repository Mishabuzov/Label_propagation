import org.apache.spark.graphx._
import scala.reflect.ClassTag


@SerialVersionUID(15L)
class LabelPropagation extends Serializable {


  def propagateLabels[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {

    val edgeDirection = EdgeDirection.Out

    /**
      * remain only graphId.
      */
    val new_graph = graph.mapVertices((vertexId, _) => vertexId)


    /**
      * Надо Отправить лэйблы соседа исходной ноде\
      */
    //TODO: CHECK IT (dif. conditions).
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {

      if (edgeDirection == EdgeDirection.Out) {

        Iterator((e.dstId, Map(e.srcAttr -> 1L)))

      } else if (edgeDirection == EdgeDirection.In) {

        Iterator((e.srcId, Map(e.dstAttr -> 1L)))

      } else {

        Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))

      }

    }

    /**
      * REDUCE
      */
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      // collect 2 sets as 1 and iterate it
      (count1.keySet ++ count2.keySet).map { i =>
        i -> (count1.getOrElse(i, 0L) + count2.getOrElse(i, 0L))
      }(collection.breakOut)
    }

    /**
      * Decide how to refresh label.
      */
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }

    val initialMessage = Map[VertexId, Long]()

    Pregel(
      new_graph,
      initialMessage,
      maxIterations = maxSteps,
      activeDirection = edgeDirection
    )(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

  }

}
