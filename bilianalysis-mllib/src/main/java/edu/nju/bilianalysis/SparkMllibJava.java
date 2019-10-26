package edu.nju.bilianalysis;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import edu.nju.bilianalysis.conf.MyConf1;
import edu.nju.bilianalysis.result.Cluster;
import edu.nju.bilianalysis.utils.json.CrawlData;
import edu.nju.bilianalysis.utils.json.MyJson;
import edu.nju.bilianalysis.utils.json.MyJsonCluster;
import edu.nju.bilianalysis.utils.mongodb.MongoUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.bson.Document;
import java.io.Serializable;
import java.util.HashMap;


public class SparkMllibJava{
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("bili-analysis-mllib");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String path = "/data/crawl_data/f1";
        JavaRDD<String> data = jsc.textFile(path);
        //prepare data
        JavaRDD<CrawlData> jsonData = data.filter(s -> s.length() > 10)
                .map(MyJson::jsonToObject)
                .filter(s -> s.view + s.barrage + s.like + s.coin + s.collect + s.share + s.comment != 0);
        JavaRDD<Vector> parsedData = jsonData.map(SparkMllibJava::objToVector);
        //sum
        double[] sums = getSum(parsedData);
        double sum = getSum(sums);
        //transform
        JavaRDD<Vector>[] parsedDatas = parsedData.randomSplit(new double[]{8, 1});
        JavaRDD<Vector> trainData = parsedDatas[0].map(s -> dealData(s, sums, sum));
        JavaRDD<Vector> testData = parsedDatas[1].map(s -> dealData(s, sums, sum));
        trainData.cache();
        // Cluster the data into two classes using KMeans
        int numClusters = 30;
        int numIterations = 10000;
        int runs = 30;
        KMeans kMeans = new KMeans();
        kMeans.setEpsilon(0.00001);
        KMeansModel clusters = KMeans.train(trainData.rdd(), numClusters, numIterations, runs);
        // Evaluate clustering by computing Within Set Sum of Squared Errors (WSSSE)
        double cost = clusters.computeCost(testData.rdd()) / testData.count();
        // Save and load model
//        FileSystem hdfs = MyHdfsUtils.getFileSystem();
//        MyHdfsUtils.delete(hdfs, "/data/test/KMeansModel");
//        clusters.save(jsc.sc(), "/data/test/KMeansModel");
//        KMeansModel sameModel = KMeansModel.load(jsc.sc(), "/data/test/KMeansModel");
        JavaPairRDD<Integer, Iterable<CrawlData>> clusterGroupRDD = jsonData.groupBy(s -> clusters.predict(dealData(objToVector(s), sums, sum)));
        JavaRDD<Cluster> clusterRDD = clusterGroupRDD.map(s -> {
                    Cluster cluster = new Cluster(s._1);
                    HashMap<String, Integer> map = new HashMap<>();
                    s._2.forEach(e -> {
                        cluster.view += e.view;
                        cluster.barrage += e.barrage;
                        cluster.like += e.like;
                        cluster.coin += e.coin;
                        cluster.collect += e.collect;
                        cluster.share += e.share;
                        cluster.comment += e.comment;
                        e.tag.forEach(t ->
                                map.merge(t, 1, (v1, v2) -> v1 + v2));
                    });
                    cluster.getSortTag(map);
                    return cluster;
                });
        //mongodb
        MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoDatabase database = client.getDatabase("mllib");
        database.drop();
        MongoCollection<Document> collectionCenter = database.getCollection("cluster_center");
        MongoCollection<Document> collectionCost = database.getCollection("cluster_cost");
        MongoCollection<Document> collectionStatistic = database.getCollection("cluster_statistic");
        for(Vector vector: clusters.clusterCenters()){
            double[] rate = vector.toArray();
            collectionCenter.insertOne(new Document("view", rate[0]).append("like_coin", rate[1])
                    .append("collect", rate[2]).append("share", rate[3]).append("comment_barrage", rate[4]));
        }
        collectionCost.insertOne(new Document("cost", cost));
        client.close();
        clusterRDD.foreach(SparkMllibJava::mongoInsertCluster);
        jsc.stop();
    }
    public static Vector dealData(Vector vector, double[] sums,double sum){
        double[] arr = vector.toArray();
        //center
        double arrSum = 0;
        for(double e: arr)  arrSum += e;
        for(int i = 0; i < arr.length; i++) {
            double ratio = (arr[i] / arrSum) / (sums[i] / sum);
            arr[i] = MyConf1.getRate(ratio);
        }
        //delete
        double[] values = new double[5];
        values[0] = arr[0];
        values[1] = (arr[2] + arr[3]) / 2;
        values[2] = arr[4];
        values[3] = arr[5];
        values[4] = (arr[1] + arr[6]) / 2;
        double valueSum = 0;
        for(double e: values)   valueSum += e;
        for(int i = 0; i < values.length; i++)  values[i] /= valueSum;
        return Vectors.dense(values);
    }
    public static Vector objToVector(CrawlData obj){
        double[] values = new double[7];
        values[0] = obj.view;
        values[1] = obj.barrage;
        values[2] = obj.like;
        values[3] = obj.coin;
        values[4] = obj.collect;
        values[5] = obj.share;
        values[6] = obj.comment;
        return Vectors.dense(values);
    }
    public static double[] getSum(JavaRDD<Vector> parsedData){
        return parsedData.reduce((x, y) -> {
            double[] values = new double[7];
            double[] arrX = x.toArray();
            double[] arrY = y.toArray();
            for(int i = 0; i < values.length; i++){
                values[i] = arrX[i] + arrY[i];
            }
            return Vectors.dense(values);
        }).toArray();
    }
    public static double getSum(double[] sums){
        double sum = 0;
        for(double e: sums){
            sum += e;
        }
        return sum;
    }
    public static void mongoInsertCluster(Cluster s){
        MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoCollection<Document> collection = client.getDatabase("mllib").getCollection("cluster_statistic");
        collection.insertOne(new Document(MyJsonCluster.objectToJson(s)));
        client.close();
    }
}