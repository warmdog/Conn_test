import java.io.File
import java.util
import java.util.Properties

import org.apache.spark.graphx
import org.apache.spark.graphx.{PartitionID, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
object  Main {
  val filePath ="/app/user/data/deeplearning/t_user_relation_phone/dt=20171008/000*"
  val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Graph").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    val value: Graph[Int,Int] = loadGraph(spark)
    //黑名单用户 缓存下来
    val graph: Graph[VertexId,Int] = getBlackSubgraph(value).cache()
//
//    val sumSingle = spark.sparkContext.longAccumulator("sumSingle")
//    val sumTwo = spark.sparkContext.longAccumulator("sumTwo")
//    val sumTriangle = spark.sparkContext.longAccumulator("sumTriangle")
    //val list = spark.sparkContext.collectionAccumulator[Long]("List[vertexInDegrees]")
    val moreThan3degrees = spark.sparkContext.collectionAccumulator[Long]("moreThan3degrees")
    val maxSubGraph = spark.sparkContext.collectionAccumulator[Long]("maxSubGraph")

//    val moreThan3degrees1 = spark.sparkContext.collectionAccumulator[Long]("moreThan3degrees")
//    val maxSubGraph1 = spark.sparkContext.collectionAccumulator[Long]("maxSubGraph")

    val partDegrees = spark.sparkContext.collectionAccumulator[Long]("partDegrees")
    val partDegrees1 = spark.sparkContext.collectionAccumulator[Long]("partDegrees")
    //spark.sparkContext
   // var min=Integer.MAX_VALUE
   // var max = 0
    //
    val blackRDD: RDD[(VertexId, PartitionID)] = graph.degrees.coalesce(100,shuffle = true)
    blackRDD.foreachPartition(x =>{
      x.foreach(y =>{
       // list.add(y._1)
        if(y._2>5){
          moreThan3degrees.add(y._1)}
   //     if(y._2>8)  moreThan3degrees1.add(y._1)
//        if(y._2==1) sumSingle.add(1)
//        if(y._2 ==2) sumTwo.add(1)
//        if(y._2 ==3) sumTriangle.add(1)
      })
    })
//    val value1: util.List[VertexId] = list.value
    val value2: util.List[VertexId] = moreThan3degrees.value
 //   val value1: util.List[VertexId] = moreThan3degrees1.value
 //   val value2: util.List[Int] = indexOfList.value
   // list.reset()
    moreThan3degrees.reset()
//    moreThan3degrees1.reset()
    //计算多少分区
//    val count = getNumberOfBlackGraph(graph,value1)
//    println(count)
//    println(s"xxxxxxxxx${count}")
    println("hello world")
    // add maxSubGraph
    addMax(graph, maxSubGraph,value2)
  //  addMax(graph,maxSubGraph1,value1)

    val maxVertex: util.List[VertexId] = maxSubGraph.value
   // val maxVertex1: util.List[VertexId] = maxSubGraph1.value

    val results: RDD[(VertexId, PartitionID,Double, List[VertexId])] = getMax1(4,graph,maxVertex,partDegrees)
    results.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/"+System.currentTimeMillis())
    println(results.count())
  //  val results1: RDD[(VertexId, PartitionID, List[VertexId])] = getMax(graph,maxVertex1)
   // results1.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/"+System.currentTimeMillis())
   val results1: RDD[(VertexId, PartitionID,Double, List[VertexId])] = getMax1(5,graph,maxVertex,partDegrees1)
    results.repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/"+System.currentTimeMillis())

    println("xxxxxxx")
    println(results1.count())
    //val value1: RDD[(String, PartitionID)] = saveNumberOfDegree(graph)
    //val number =value1
//    number.saveAsTextFile("/app/user/data/deeplearning/results/")
//    number.foreach(x =>{
//      println(x)
//    })
  //numberOfGraph.add(count)
  }

  def getMax1(int: Int,graph: Graph[VertexId,Int],maxVertex:util.List[VertexId],collectionAccumulator: CollectionAccumulator[Long]):RDD[(VertexId, PartitionID, Double,List[VertexId])] ={
    val degreesMap = mutable.HashMap[VertexId,Int]()
    val value1: Graph[VertexId, PartitionID] = graph.subgraph(vpred = (id,value) => maxVertex.contains(id))
    value1.degrees.foreachPartition(x =>{
      x.foreach(y =>{
        if(y._2>int){
          collectionAccumulator.add(y._1)
        }
      })
    })
    val value2: util.List[VertexId] = collectionAccumulator.value
    val value3 = value1.subgraph(vpred = (id,value) => value2.contains(id))
    value3.degrees.foreachPartition(x =>{
      x.foreach(y =>{
        degreesMap.put(y._1,y._2)
      })
    })
    val a: RDD[(VertexId, PartitionID, Double,List[VertexId])] =value3.connectedComponents().vertices.mapPartitions(x => {
      val map = mutable.HashMap[VertexId, List[VertexId]]()
      x.foreach(y => {
        if (map.contains(y._2)) {
          // val long = List[Long](map.get(y._2))
          val tmp = (y._1) :: map.get(y._2).getOrElse(List(0L))
          map.put(y._2, tmp)
        } else {
          map.put(y._2, List(y._1))
        }
      })
      map.iterator
    }).reduceByKey((x, y) => x ++ y).map(x => {
      var sum =0
      for (y <- x._2){
        var degree = degreesMap.get(y).getOrElse(0)
        sum =degree + sum
      }
      var score =0.0
      if(x._2.size>1){
        score = sum/(x._2.size*(x._2.size-1))
      }
      (x._1, x._2.size,score, x._2)
    })
    a
  }
    //获取黑名单中 最大子图的相关信息 1000000313132056209L 为最大子图的最少id
    /*
    * 1先把最大子图个顶点保存到 accumulate 中1
    * 2然后 从black图中筛选出 顶点在accumulate 中的图2
    *
    * */
    // 2
    def getMax(graph: Graph[VertexId,Int],maxVertex:util.List[VertexId]):RDD[(VertexId, PartitionID, List[VertexId])] ={
      val value1: Graph[VertexId, PartitionID] = graph.subgraph(vpred = (id,value) => maxVertex.contains(id))
      val a: RDD[(VertexId, PartitionID, List[VertexId])] =value1.connectedComponents().vertices.mapPartitions(x => {
        val map = mutable.HashMap[VertexId, List[VertexId]]()
        x.foreach(y => {
          if (map.contains(y._2)) {
            // val long = List[Long](map.get(y._2))
            val tmp = (y._1) :: map.get(y._2).getOrElse(List(0L))
            map.put(y._2, tmp)
          } else {
            map.put(y._2, List(y._1))
          }
        })
        map.iterator
      }).reduceByKey((x, y) => x ++ y).map(x => {
        (x._1, x._2.size, x._2)
      })
      a
    }
    //1
    def addMax(graph: Graph[VertexId,Int],maxSub: CollectionAccumulator[Long],list2:util.List[VertexId]):Unit ={
      graph.connectedComponents().vertices.filter(x => x._2==1000000313132056209L &&list2.contains(x._1)).foreachPartition(x =>{
        x.foreach(y =>{
          maxSub.add(y._1)
        })
      })
    }
    //获取黑名单中 有多少个{子图 子图的大小 子图内的点vertexID}
    def getDetailedSubgraphInfo(graph: Graph[VertexId,Int],list1: util.List[ VertexId],list2:util.List[Int]):RDD[(VertexId, PartitionID, List[VertexId])] ={
      val a: RDD[(VertexId, PartitionID, List[VertexId])] = graph.connectedComponents().vertices.filter(x => list1.contains(x._1)).mapPartitions(x => {
        val map = mutable.HashMap[VertexId, List[VertexId]]()
        x.foreach(y => {
            if (map.contains(y._2)) {
              // val long = List[Long](map.get(y._2))
              val tmp = (y._1) :: map.get(y._2).getOrElse(List(0L))
              map.put(y._2, tmp)
            } else {
              map.put(y._2, List(y._1))
            }
        })
        map.iterator
      }).reduceByKey((x, y) => x ++ y).map(x => {
        (x._1, x._2.size, x._2)
      })
      a
    }

  // 加载图
  def loadGraph(sparkSession: SparkSession):Graph[Int,Int] ={
    val fileRDD: RDD[String] = sparkSession.sparkContext.textFile(filePath)
    val edgeRDD = fileRDD.map(x =>{
      val item= x.trim.split("\\s+")
      Edge(item(0).toLong,item(1).toLong,item(3).toInt)
    })
    val blackDataSet: Dataset[Row] = sparkSession.sql("select phone from blacklist.black_type_list_new")
    val blackRDD: RDD[Row] = blackDataSet.rdd
    val regex = "[0-9]".r
    val vertexRDD: RDD[(VertexId, Int)] = blackRDD.map(x => {
      val s = x.toString().trim
      //hive 获取多行数据方法 val value = x.getAs[String](1)
      //val num = x.getAs[Long](2)
      ((regex findAllIn s).mkString("").toLong, Integer.valueOf(1))
    })
    val value: Graph[PartitionID, PartitionID] = Graph.fromEdges(edgeRDD, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel =
      StorageLevel.MEMORY_AND_DISK_SER)
    //Graph(vertexRDD,edgeRDD)
    val result: Graph[PartitionID, PartitionID] = value.outerJoinVertices(vertexRDD) { (id, oldAttr, outDegOpt) =>
//      outDegOpt match {
//        case Some(outDegOpt) => outDegOpt
//        case None =>oldAttr
//      }
      outDegOpt.getOrElse(0)
    }
    result
  }
  //def getEverySubgraph(graph: Graph[Int,Int],list: util.List[VertexId]):
  //获取黑名单子图
  def getBlackSubgraph(graph: Graph[Int,Int]) :Graph[VertexId,Int] ={
    val ccGraph: Graph[VertexId, PartitionID] = graph.connectedComponents()
    val black: Graph[PartitionID, PartitionID] = graph.subgraph(vpred = (vertexId, value) => value == 1)
    //no longer have missing field
    val blackGraph = ccGraph.mask(black)
    blackGraph
  }
  def getNumberOfBlackGraph(graph: Graph[VertexId,Int],list: util.List[VertexId]): Long={
    val count: VertexId = graph.connectedComponents().vertices.map(x =>
      if(list.contains(x._2)) {x._2}
      else { 0L}
    ).distinct().count()
    count
  }
  //黑名单子图中，度的大小和对应的个数
  def saveNumberOfDegree(graph: Graph[VertexId,Int]):  RDD[(String, PartitionID)]={
    val result: RDD[(String, PartitionID)] = graph.degrees.mapPartitions(x => {
      var map = mutable.HashMap[String,PartitionID]()
      while (x.hasNext) {
        val cul = x.next()._2.toString
        if (!map.contains(cul)) {
          map.put(cul, 1)
        }
        else {
          val num = map.get(cul).get + 1
          map.put(cul, num)
        }
      }
      map.iterator
    }).repartition(100).reduceByKey((x, y) => x + y).repartition(1)
    result
  }
}
