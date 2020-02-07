package edu.nju.bilianalysis;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import edu.nju.bilianalysis.conf.MyConf1;
import edu.nju.bilianalysis.utils.json.CrawlData;
import edu.nju.bilianalysis.utils.json.MyJson;
import edu.nju.bilianalysis.utils.mongodb.MongoUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import scala.Tuple2;

import java.util.*;

public class SparkStreamingJava {
    public static void main(String [] args){
        SparkConf conf = new SparkConf().setAppName("bili-analysis-streaming");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaDStream<String> lines = ssc.textFileStream(MyConf1.HDFS_STREAMING_PATH);
        JavaDStream<CrawlData> crawlDataRDD = lines.filter(s -> s.length() > 10).map(MyJson::jsonToObject);

        JavaPairDStream<String,Double> timeNum = getTimeTag(crawlDataRDD);
        JavaPairDStream<String,Double> globalView = getTagWeight(crawlDataRDD, new float[]{1f,0,0,0,0,0,0});
        JavaPairDStream<String,Double> globalHot = getTagWeight(crawlDataRDD, new float[]{0,0.8f,0.1f,0.2f,0.2f,1f,2f});
        JavaPairDStream<String,Double> timeNumReduce = timeNum.reduceByKey((x, y) -> x + y);
        JavaPairDStream<String,Double> globalViewReduce = globalView.reduceByKey((x, y) -> x + y);
        JavaPairDStream<String,Double> globalHotReduce = globalHot.reduceByKey((x, y) -> x + y);

        timeNumReduce.foreachRDD(rdd -> rdd.foreach(s -> mongoUpdate(s, "time_view", "time", "view")));
        globalViewReduce.foreachRDD(rdd -> rdd.foreach(s -> mongoUpdate(s, "tag_view", "tag", "view")));
        globalHotReduce.foreachRDD(rdd -> rdd.foreach(s -> mongoUpdate(s, "tag_hot", "tag", "hot")));

        ssc.start();
        try{
            ssc.awaitTermination();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static JavaPairDStream<String,Double> getTimeTag(JavaDStream<CrawlData> crawlDataRDD){
        JavaPairDStream<String, CrawlData> timePair = crawlDataRDD.mapToPair(s->{
            Tuple2<String, CrawlData> tuple2 = new Tuple2<>(s.time.substring(0, 10),s);
            return tuple2;
        });
        JavaPairDStream<String,Iterable<CrawlData>> timePairs = timePair.groupByKey();
        JavaPairDStream<String,Double> timeCount = timePairs.mapToPair(s->{
            double count = 0;
            for(CrawlData obj: s._2){
                count++;
            }
            Tuple2<String, Double> tuple2 = new Tuple2<>(s._1,count);
            return tuple2;
        });
        return timeCount;
    }
    public static JavaPairDStream<String,Double> getTagWeight(JavaDStream<CrawlData> crawlDataRDD, float[] weights){
        JavaPairDStream<String, CrawlData> tag_crawlData = crawlDataRDD.flatMap(new FlatMapFunction<CrawlData, Object>() {
        }).flatMapToPair(s->{
            List<Tuple2<String, CrawlData>> list = new ArrayList<>();
            for (String temp : s.tag) {
                list.add(new Tuple2<>(temp,s));
            }
            return list.iterator();
        });
        JavaPairDStream<String,Iterable<CrawlData>> tag_listCrawlData = tag_crawlData.groupByKey();
        // (tag,weight)
        JavaPairDStream<String,Double> tag_weight = tag_listCrawlData.mapToPair(s->{
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

    public static void mongoUpdate(Tuple2<String,Double> s, String collectionName, String n1, String n2){
        try{
            MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
            MongoCollection<Document> collection = client.getDatabase("streaming").getCollection(collectionName);
            Document document = collection.find(Filters.eq(n1, s._1)).first();
            if(document == null){
                collection.insertOne(new Document(n1, s._1).append(n2, s._2));
            }else{
                Double num = document.getDouble(n2);
                collection.replaceOne(Filters.eq(n1, s._1), new Document(n1, s._1).append(n2, s._2 + num));
            }
            client.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
