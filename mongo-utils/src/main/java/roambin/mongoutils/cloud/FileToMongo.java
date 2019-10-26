package roambin.mongoutils.cloud;

import roambin.mongoutils.cloud.conf.MyConf1;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import roambin.mongoutils.json.MyJson;
import org.bson.Document;
import roambin.mongoutils.utils.MongoUtils;
import java.io.BufferedReader;
import java.io.FileReader;

public class FileToMongo {
    public static void main(String[] args) throws Exception{
//        FileWriter fileWriter = new FileWriter("data2.txt");
        int a = 0/0;
        MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoCollection<Document> mongoCollection = client.getDatabase("crawl").getCollection("data");
        FileReader fileReader = new FileReader(MyConf1.DATAFILE_PATH);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;
        int count = 0;
        while((line = bufferedReader.readLine()) != null){
            if(++count % 10 == 0)   System.out.println(count);
            if(line.length() > 10){
                line = MyJson.objectToJson(MyJson.jsonToObject(line)).toJSONString();
//                mongoCollection.insertOne(Document.parse(line));
//                fileWriter.write(line + "\n");
            }
        }
        bufferedReader.close();
        fileReader.close();
//        fileWriter.close();
    }
}
