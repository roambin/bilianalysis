import java.lang.String;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkStreaming001 {
    public static void main(String [] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TestFileStreaming");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lineFile_filter =
                sc.textFile("hdfs://172.19.240.230:9000/data/crawl_data/f1").filter(s -> s.length() > 10);

        JavaPairRDD<String,CrawlData> tag_crawlData = lineFile_filter.flatMapToPair(s->{

            CrawlData crawlData = MyJson.jsonToObject(s);
            List<Tuple2<String,CrawlData>> list = new ArrayList<>();
            for (String temp : crawlData.tag) {
                if(temp.length()>1)
                    list.add(new Tuple2<>(temp,crawlData));
            }
            return list.iterator();
        });
        JavaPairRDD<String,Iterable<CrawlData>> tag_listCrawlData = tag_crawlData.groupByKey();

        // (tag,weight)
        JavaPairRDD<String,Double> tag_weight = tag_listCrawlData.mapToPair(s->{
            double weightSum = 0;

            for (CrawlData crawlData : s._2) {
                double weight = (crawlData.like *0.1+crawlData.coin*0.2+crawlData.collect*0.2+
                        crawlData.share+crawlData.comment*2+crawlData.view*0.01+crawlData.barrage*0.8);
                weightSum+=weight;
            }
            return new Tuple2<>(s._1(),weightSum);
        });

        // sort by weight
        JavaPairRDD<Double,String> weight_tag = tag_weight.mapToPair(s->s.swap()).sortByKey();
        tag_weight=weight_tag.mapToPair(s->s.swap());


        tag_weight.foreach(s->{
            System.out.println(s);
        });
    }
}
