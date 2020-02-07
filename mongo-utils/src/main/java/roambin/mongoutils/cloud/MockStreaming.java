package roambin.mongoutils.cloud;

import roambin.mongoutils.cloud.conf.MyConf1;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.fs.FileSystem;
import org.bson.Document;
import roambin.mongoutils.utils.HdfsUtils;
import roambin.mongoutils.utils.MongoUtils;
import java.io.*;
import java.util.function.Consumer;

public class MockStreaming {
    public static void main(String[] args) throws Exception{
        //read file
        BufferedReader bufferedReader = new BufferedReader(new FileReader(MyConf1.MOCKSTREAMING_CONTROL_FILE));
        int onOff = readNextNum(bufferedReader);
        int avNum = readNextNum(bufferedReader);
        int loop = readNextNum(bufferedReader);
        int step = readNextNum(bufferedReader);
        int sleepTime = readNextNum(bufferedReader);
        int append = readNextNum(bufferedReader);
        bufferedReader.close();
        writeControl(1, avNum, loop, step, sleepTime, append);
        //hdfs
        FileSystem fileSystem = HdfsUtils.getFileSystem();
        //mongo
        MongoClient mongoClient = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase("crawl").getCollection("data");
        for(int i = 1; i <= loop; i++){
            if(isOff()) break;
            if(append == 1)    writeControl(1, avNum + i * step, loop, step, sleepTime, append);
            int newAvNum = avNum + (i - 1) * step;
            FindIterable<Document> findIterable = mongoCollection.find(
                    new Document().append("av_num", new Document("$gte", newAvNum).append("$lt", newAvNum + step)));
            //findIterable.forEach((Consumer<Document>)System.out::println);
            StringBuffer stringBuffer = new StringBuffer();
            findIterable.forEach((Consumer<Document>)e -> stringBuffer.append(e.toJson()).append('\n'));
            //hdfs
            if(stringBuffer.length() > 0){
                HdfsUtils.putString(fileSystem, new String(stringBuffer), "/data/streaming/m" + newAvNum, true);
                System.out.println("input: m" + newAvNum + ", length: " + step);
                System.out.println(new String(stringBuffer));
            }
            Thread.sleep(sleepTime);
        }
        //close
        fileSystem.close();
        mongoClient.close();
    }
    public static void writeControl(int onOff, int avNum, int loop, int step, int sleepTime, int append) throws Exception{
        FileWriter fileWriter = new FileWriter(MyConf1.MOCKSTREAMING_CONTROL_FILE);
        fileWriter.write("on/off: " + onOff
                + "\navNum: " + avNum
                + "\nloop: " + loop
                + "\nstep: " + step
                + "\nsleep time: " + sleepTime
                + "\nappend: " + append);
        fileWriter.close();
    }
    public static boolean isOff() throws Exception{
        BufferedReader bufferedReader = new BufferedReader(new FileReader(MyConf1.MOCKSTREAMING_CONTROL_FILE));
        int onOff = readNextNum(bufferedReader);
        return onOff == 0;
    }
    public static int readNextNum(BufferedReader bufferedReader) throws Exception{
        return Integer.parseInt(bufferedReader.readLine().replaceAll("\\D", ""));
    }
}
