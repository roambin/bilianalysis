package edu.nju.bilianalysis.conf;

public class MyConf1 {
    public static final String MONGO_CONNECTION = "mongodb://root:123@172.19.240.108:27017/admin?w=majority";
    public static final String HDFS_DATA_PATH = "hdfs://172.19.240.230:9000/data/crawl_data/f1";
    public static final String HDFS_STREAMING_PATH = "hdfs://172.19.240.230:9000/data/streaming";

    public static int getRate(double rate){
        if(rate <= 0.1){
            return 0;
        }else if(rate > 0.1 && rate <= 1 / 3){
            return 400;
        }else if(rate > 1 / 3 && rate <= 3){
            return 500;
        }else if(rate > 3 && rate <= 10){
            return 600;
        }else{
            return 1000;
        }
    }
}
