package edu.nju.bilianalysis;
import edu.nju.bilianalysis.utils.json.CrawlData;
import edu.nju.bilianalysis.utils.json.MyJson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;


import java.util.*;

public class BSparkGraphxJava {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines =
                sc.textFile("hdfs://172.19.240.230:9000/data/crawl_data/f1").filter(s -> s.length() > 10);
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ClassTag<CrawlData> crawlDataClassTag = scala.reflect.ClassTag$.MODULE$.apply(CrawlData.class);
        JavaRDD<CrawlData> crawlDataRDD = lines.filter(s -> s.length() > 10).map(MyJson::jsonToObject).filter(s->s.tag.size()>1);
        // get all tags
        JavaRDD<Iterable<String>> tag_list = crawlDataRDD.map(s->s.tag);
        // single tag remove same
        JavaRDD<String> tag_flat = tag_list.flatMap(s->s.iterator()).map(s->{
            return s.toUpperCase();
        }).distinct();
        // set turn for each vertices
        JavaPairRDD<String,Long> tag_no = tag_flat.zipWithIndex();
        // init vertices
        JavaRDD<Tuple2<Object,String>> verticesRDD = tag_no.map(s->{
            return new Tuple2<>(s._2(),s._1());
        });
        // init edge
        Map<String,Long> tag_noMap = tag_no.collectAsMap();
        JavaRDD<Edge<CrawlData>> edgeRDD = crawlDataRDD.map(s->{
            List<Edge<CrawlData>> edges = new ArrayList<>();
            for(int i =0; i<s.tag.size()-1;i++){
                for(int j = i+1 ; j<s.tag.size();j++){
                    if(tag_noMap.get(s.tag.get(i))!=null && tag_noMap.get(s.tag.get(j))!=null)
                    edges.add(new Edge<CrawlData>(tag_noMap.get(s.tag.get(i)),tag_noMap.get(s.tag.get(j)),s));
                }
            }
            return edges.iterator();
        }).flatMap(s->s);
        edgeRDD.collect();
        Graph<String,CrawlData> graph = Graph.apply(verticesRDD.rdd(),edgeRDD.rdd(),
                "",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                stringTag,crawlDataClassTag);
        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
    }

    public static JavaPairRDD<String,Double> getTagWeight(JavaRDD<CrawlData> crawlDataRDD, float[] weights){
        // list <(tag,crawlObject)>
        JavaPairRDD<String, CrawlData> tag_crawlData = crawlDataRDD.flatMapToPair(s->{
            List<Tuple2<String, CrawlData>> list = new ArrayList<>();
            for (String temp : s.tag) {
                list.add(new Tuple2<>(temp,s));
            }
            return list.iterator();
        });
        // (tag,list<crawlObject>)
        JavaPairRDD<String,Iterable<CrawlData>> tag_listCrawlData = tag_crawlData.groupByKey();
        // (tag,weight)
        JavaPairRDD<String,Double> tag_weight = tag_listCrawlData.mapToPair(s->{
            double weightSum = 0;
            for (CrawlData crawlData : s._2) {
                double weight = (crawlData.view * weights[0]
                        + crawlData.barrage * weights[1]
                        + crawlData.like * weights[2]
                        + crawlData.coin * weights[3]
                        + crawlData.collect * weights[4]
                        + crawlData.share * weights[5]
                        + crawlData.comment * weights[6]);
                weightSum+=weight;
            }
            return new Tuple2<>(s._1(),weightSum);
        });
        return tag_weight;
    }

}
