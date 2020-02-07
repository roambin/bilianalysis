package edu.nju.bilianalysis

import java.io.{File, PrintWriter}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import edu.nju.bilianalysis.utils.conf.MyConf1
import edu.nju.bilianalysis.utils.json.{CrawlData, MyJson, VertexValue}
import edu.nju.bilianalysis.utils.mongodb.MongoUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.bson.Document
import scala.collection.mutable.ListBuffer

object SparkGraphxScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("graph")
    conf.set("spark.mongodb.input.uri", MyConf1.MONGO_CONNECTION)
      .set("spark.mongodb.input.database","crawl")
      .set("spark.mongodb.input.collection","data")
    val sc = new SparkContext(conf)
    val mongoRDD: MongoRDD[Document] = MongoSpark.load(sc)
    val crawlDataRDD = mongoRDD.map(s => MyJson.jsonToObject(s.toJson))
    //    val lines = sc.textFile("hdfs://172.19.240.230:9000/data/crawl_data/f1").filter(s => s.length > 10)
    //    val crawlDataRDD = lines.map[CrawlData](MyJson.jsonToObject).filter((s: CrawlData) => s.tag.size > 0)
    val tag_obj = crawlDataRDD.flatMap[(String, CrawlData)](s => {
      val listBuffer = ListBuffer[(String, CrawlData)]()
      s.tag.toArray[String](new Array[String](0)).foreach(t => listBuffer.append((t.toLowerCase, s)))
      listBuffer
    })
    val tag_objs = tag_obj.groupByKey
    val tagRdd = tag_objs.map[String](s => s._1).zipWithUniqueId()
    val tagMap = tagRdd.collectAsMap
    val vertexIdMap = tagRdd.map[(VertexId, String)](_.swap).collectAsMap()
    val rddEdge = tag_obj.flatMap[Edge[CrawlData]](s => {
        val edges = ListBuffer[Edge[CrawlData]]()
        s._2.tag.toArray[String](new Array[String](0)).foreach(t => {
          if (!(t == s._1)){
            edges.append(new Edge[CrawlData](tagMap(s._1.toLowerCase()), tagMap(t.toLowerCase), s._2))
          }
        })
        edges
    })
    val rddVertex = tagRdd.map[(VertexId, VertexValue)](s => (s._2, new VertexValue(s._1)))
//    val edge = EdgeRDD.fromEdges(rddEdge)
//    val vertex = VertexRDD.apply(rddVertex)
    //graphx
    var graph = Graph.apply[VertexValue, CrawlData](rddVertex, rddEdge)
    graph = graph.groupEdges((s1, s2) => CrawlData.sum(s1, s2))
//    storeGexf(graph, "bili_graph.gexf")
    //clear mongodb
    mongoClear()
    mongoInsert("statistic", Map("vertex" -> graph.vertices.count(), "edge" -> graph.edges.count()))
    //count neighbours
    val neighbourCount = graph.aggregateMessages[Int](triple => triple.sendToDst(1), (o1, o2) => o1 + o2)
      .sortBy(_._2, false)
    val neighbourCountRdd = neighbourCount.map(s => (vertexIdMap(s._1), s._2))
    neighbourCountRdd.take(20).foreach(s => mongoInsert("neighbour_count", Map("tag" -> s._1, "statistic" -> s._2)))
    //triangle count
    val triangleCountVertices = graph.triangleCount
      .vertices
      .sortBy(_._2, false)
    val triangleCountRdd = triangleCountVertices.map(s => (vertexIdMap(s._1), s._2))
    triangleCountRdd.take(20).foreach(s => mongoInsert("triangle_countList", Map("tag" -> s._1, "statistic" -> s._2)))
    //page rank
    val pageRankVertices = graph.pageRank(0.001, 0.15)
      .vertices
      .sortBy(_._2, false)
    val pageRankRdd = pageRankVertices.map(s => (vertexIdMap(s._1), s._2))
    pageRankRdd.take(20).foreach(s => mongoInsert("page_rank", Map("tag" -> s._1, "statistic" -> s._2)))
    //components vertices
    val connectedComponentsVertices = graph.connectedComponents
      .vertices
      .map(_.swap)
      .groupByKey()
      .map(_._2)
    val singleVertex = connectedComponentsVertices.filter(s => s.size == 1).map(s => s.toArray).map(s => s(0))
    val singleVertexRdd = singleVertex.map(vertexIdMap(_))
    singleVertexRdd.foreach(s => mongoInsert("single_vertex", Map("tag" -> s)))
    //generate gexf
    val nodeList = triangleCountVertices.map(_._1).take(200)
    val rddVertexSlim = graph.vertices.filter(s => nodeList.contains(s._1))
    val rddEdgeSlim = graph.edges.filter(s => nodeList.contains(s.srcId) && nodeList.contains(s.dstId))
    val graphSlim = Graph.apply[VertexValue, CrawlData](rddVertexSlim, rddEdgeSlim)
    storeGexf(graphSlim, "bili_graph_slim.gexf")
  }

  def mongoClear(): Unit = {
    val client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION)
    val database = client.getDatabase("graphx")
    database.drop()
    client.close()
  }
  def mongoInsert(table: String, elem: Map[String, Any]): Unit = {
    val client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION)
    val collection = client.getDatabase("graphx").getCollection(table)
    val document = new Document()
    elem.foreach(s => document.append(s._1, s._2))
    collection.insertOne(document)
    client.close()
  }
  def toGexf(g:Graph[VertexValue,CrawlData]) : String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "    <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "        <nodes>\n" +
      g.vertices.map(v => "            <node id=\"" + v._1 + "\" label=\"" +
        v._2.tag + "\" />\n").collect.mkString +
      "        </nodes>\n" +
      "        <edges>\n" +
      g.edges.map(e => "            <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr.view +
        "\" />\n").collect.mkString +
      "        </edges>\n" +
      "    </graph>\n" +
      "</gexf>"
  }
  def storeGexf(graph:Graph[VertexValue,CrawlData], filename:String) : Unit = {
    val gexf = toGexf(graph)
    val fileWriter = new PrintWriter(new File(filename))
    fileWriter.print(gexf)
    fileWriter.close()
  }
}
