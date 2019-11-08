package edu.nju.bilianalysis;
import edu.nju.bilianalysis.utils.json.CrawlData;
import edu.nju.bilianalysis.utils.json.MyJson;
import edu.nju.bilianalysis.utils.json.VertexValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;
import java.util.*;

public class SparkGraphxJava implements Serializable {

    public static void main(String [] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines =
                sc.textFile("hdfs://172.19.240.230:9000/data/test/f1").filter(s -> s.length() > 10);
        ClassTag<VertexValue> verticeValueTag = ClassTag$.MODULE$.apply(VertexValue.class);
        ClassTag<CrawlData> crawlDataClassTag = ClassTag$.MODULE$.apply(CrawlData.class);
        ClassTag<Integer> intDataClassTag = ClassTag$.MODULE$.apply(Integer.class);

        JavaRDD<CrawlData> crawlDataRDD = lines.filter(s -> s.length() > 10).
                map(MyJson::jsonToObject).filter(s -> s.tag.size() > 0);
        JavaPairRDD<String, CrawlData> tag_obj = crawlDataRDD.flatMapToPair(s -> {
            List<Tuple2<String, CrawlData>> list = new ArrayList<>();
            s.tag.forEach(t -> list.add(new Tuple2<>(t.toLowerCase(), s)));
            return list.iterator();
        });
        JavaPairRDD<String, Iterable<CrawlData>> tag_objs = tag_obj.groupByKey();
        JavaPairRDD<String, Long> tagRdd = tag_objs.map(s -> s._1).zipWithIndex();
        Map<String ,Long> tagMap = tagRdd.collectAsMap();

        JavaRDD<Edge<CrawlData>> edgeRDD = tag_obj.flatMap(s -> {
            List<Edge<CrawlData>> edges = new ArrayList<>();
            s._2.tag.forEach(t -> {
                if(!t.equals(s._1)){
                    edges.add(new Edge<>(tagMap.get(s._1), tagMap.get(t), s._2));
                }
            });
            return edges.iterator();
        });
//        JavaRDD<Edge<CrawlData>> edgeRDD = edgeRDDFlat
//                .groupBy(s -> s.srcId() + "|" + s.dstId())
//                .map(s -> {
//                    String[] index = s._1.split("\\|");
//                    CrawlData crawlData = new CrawlData();
//                    s._2.forEach(e -> {
//                        crawlData.add(e.attr);
//                    });
//                    return new Edge<>(Long.parseLong(index[0]), Long.parseLong(index[1]), crawlData);
//                });
        JavaRDD<Tuple2<Object, VertexValue>> verticesRDD = tagRdd.map(s -> new Tuple2<>(s._2, new VertexValue(s._1)));
        Graph<VertexValue,CrawlData> graph = Graph.apply(verticesRDD.rdd(),edgeRDD.rdd(), new VertexValue(),
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                verticeValueTag,crawlDataClassTag);
        graph.groupEdges((s1, s2) -> CrawlData.sum(s1, s2));
        VertexRDD<VertexValue> verticeWholeView = graph.aggregateMessages(new SendMsgFunction(),new MergeMsgFunction()
                ,TripletFields.Dst, ClassTag$.MODULE$.apply(VertexValue.class));
        Graph<VertexValue,CrawlData> graphWholeVeiw = Graph.apply(verticeWholeView.toJavaRDD().rdd(),edgeRDD.rdd(), new VertexValue(),
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                verticeValueTag,crawlDataClassTag);

    }

    static class SendMsgFunction extends AbstractFunction1<EdgeContext<VertexValue, CrawlData, VertexValue>, BoxedUnit> implements Serializable{
        @Override
        public BoxedUnit apply(EdgeContext<VertexValue, CrawlData, VertexValue> edgeCtx) {
            edgeCtx.sendToDst(edgeCtx.dstAttr());
            return BoxedUnit.UNIT;
        }
    }
    static class MergeMsgFunction extends AbstractFunction2<VertexValue, VertexValue, VertexValue> implements Serializable{
        @Override
        public VertexValue apply(VertexValue o1, VertexValue o2) {
            return new VertexValue().add(o1).add(o2);
        }
    }
    static class MapValuesFunction extends AbstractFunction1<Object, Object> implements Serializable{
        @Override
        public Object apply(Object obj) {
            Tuple2<Integer, Long> t = (Tuple2<Integer, Long>)obj;
            int count = t._1;
            long age = t._2;
            return age/count;
        }
    }
}

