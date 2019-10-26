package edu.nju.bilianalysis;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import edu.nju.bilianalysis.conf.MyConf1;
import edu.nju.bilianalysis.result.TimeVedio;
import edu.nju.bilianalysis.utils.mongodb.MongoUtils;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import edu.nju.bilianalysis.utils.json.CrawlData;
import edu.nju.bilianalysis.utils.json.MyJson;

import java.util.*;

public class SparkJava {
    public static void main(String [] args){
        SparkConf conf = new SparkConf().setAppName("bili-analysis-data");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineJson = sc.textFile(MyConf1.HDFS_DATA_PATH);
        JavaRDD<String> lineJsonFilter = lineJson.filter(s -> s.length() > 10);
        JavaRDD<CrawlData> crawlDataRDD = lineJsonFilter.map(MyJson::jsonToObject);

        JavaPairRDD<String, TimeVedio> timeTag = getTimeTag(crawlDataRDD);
        JavaPairRDD<String, Double> globalView = getTagWeight(crawlDataRDD, new float[]{1f,0,0,0,0,0,0});
        JavaPairRDD<String, Double> globalHot = getTagWeight(crawlDataRDD, new float[]{0,0.8f,0.1f,0.2f,0.2f,1f,2f});
//        JavaPairRDD<String, TimeVedio> timeTagReduce = timeTag.reduceByKey((x, y) -> {
//           TimeVedio timeVedio = new TimeVedio();
//           timeVedio.count = x.count + y.count;
//           timeVedio.tabList.addAll(x.tabList);
//           timeVedio.tabList.addAll(y.tabList);
//           return timeVedio;
//        });
//        JavaPairRDD<String,Double> globalViewReduce = globalView.reduceByKey((x, y) -> x + y);
//        JavaPairRDD<String,Double> globalHotReduce = globalHot.reduceByKey((x, y) -> x + y);
        //mongo
        MongoUtils.getClient(MyConf1.MONGO_CONNECTION).getDatabase("total").drop();
        timeTag.foreach(s -> mongoInsertTime(s, "time_view", "time", "view", "tags"));
        globalView.foreach(s -> mongoInsert(s, "tag_view", "tag", "view"));
        globalHot.foreach(s -> mongoInsert(s, "tag_hot", "tag", "hot"));
    }
    public static JavaPairRDD<String, TimeVedio> getTimeTag(JavaRDD<CrawlData> crawlDataRDD){
        JavaPairRDD<String, Iterable<CrawlData>> timePair = crawlDataRDD.groupBy(o -> o.time.substring(0, 10));
        JavaPairRDD<String, TimeVedio> timeVedioRdd = timePair.mapValues(s->{
            HashMap<String, Integer> map = new HashMap<>();
            int count = 0;
            for(CrawlData o: s){
                count++;
                o.tag.forEach(t->map.merge(t, 1, (prev, one) -> prev + one));
            }
            ArrayList<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, (o1, o2)-> o2.getValue().compareTo(o1.getValue()));
            ArrayList<String> tagList = new ArrayList<>();
            for(int i = 0; i < 10 && i < list.size(); i++){
                tagList.add(list.get(i).getKey());
            }
            return new TimeVedio(count, tagList);
        });
        return timeVedioRdd;
    }
    public static JavaPairRDD<String, Double> getTagWeight(JavaRDD<CrawlData> crawlDataRDD, float[] weights){
        JavaPairRDD<String, CrawlData> tag_crawlData = crawlDataRDD.flatMapToPair(s->{
            List<Tuple2<String, CrawlData>> list = new ArrayList<>();
            for (String temp : s.tag) {
                list.add(new Tuple2<>(temp,s));
            }
            return list.iterator();
        });
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
        // sort by weight
        JavaPairRDD<Double,String> weight_tag = tag_weight.mapToPair(s->s.swap()).sortByKey(false);
        tag_weight=weight_tag.mapToPair(s->s.swap());
        return tag_weight;
    }
    public static void mongoInsert(Tuple2<String, Double> s, String collectionName, String n1, String n2){
        MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoCollection<Document> collection = client.getDatabase("total").getCollection(collectionName);
        collection.insertOne(new Document(n1, s._1).append(n2, s._2));
        client.close();
    }
    public static void mongoInsertTime(Tuple2<String, TimeVedio> s, String collectionName, String n1, String n2, String n3){
        MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoCollection<Document> collection = client.getDatabase("total").getCollection(collectionName);
        collection.insertOne(new Document(n1, s._1).append(n2, s._2.count).append(n3, s._2.tabList));
        client.close();
    }
}
