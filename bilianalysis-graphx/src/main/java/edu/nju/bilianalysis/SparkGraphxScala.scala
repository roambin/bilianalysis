package edu.nju.bilianalysis

import java.util.ArrayList

import edu.nju.bilianalysis.utils.json.{CrawlData, MyJson, VertexValue}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.graphx._


object SparkGraphxScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("graph")
    val sc = new JavaSparkContext(conf)
    val lines = sc.textFile("hdfs://172.19.240.230:9000/data/test/f1").filter(s => s.length > 10)
    val crawlDataRDD = lines.map[CrawlData](MyJson.jsonToObject).filter((s: CrawlData) => s.tag.size > 0)
    val tag_obj = crawlDataRDD.flatMapToPair[String, CrawlData](s => {
        val list:ArrayList[Tuple2[String, CrawlData]] = new ArrayList
        s.tag.forEach((t: String) => list.add(new Tuple2[String, CrawlData](t.toLowerCase, s)))
        list.iterator
    })
    val tag_objs = tag_obj.groupByKey
    val tagRdd = tag_objs.map[String](s => s._1).zipWithIndex
    val tagMap = tagRdd.collectAsMap
    val rddEdge = tag_obj.flatMap[Edge[CrawlData]](s => {
        val edges:ArrayList[Edge[CrawlData]] = new ArrayList
        s._2.tag.forEach(t => {
          if (!(t == s._1)){
            edges.add(new Edge[CrawlData](tagMap.get(s._1), tagMap.get(t), s._2))
          }
        })
        edges.iterator
    })
    val rddVertex = tagRdd.map[(Long, VertexValue)](s => (s._2, new VertexValue(s._1)))
//    val edge = EdgeRDD.fromEdges(rddEdge)
//    val vertex = VertexRDD.apply(rddVertex)
    var graph = Graph.apply[VertexValue, CrawlData](rddVertex, rddEdge)
    graph = graph.groupEdges((s1, s2) => CrawlData.sum(s1, s2))
    val vertexSum = graph.aggregateMessages[Int](triple => triple.sendToDst(1), (o1, o2) => o1 + o2)
    vertexSum
  }
}
