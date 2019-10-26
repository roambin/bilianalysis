package roambin.mongoutils;

import com.mongodb.ConnectionString;
import com.mongodb.client.*;
import com.mongodb.MongoClientSettings;
import org.bson.Document;

import java.util.function.Consumer;

public class MongoPlayGround {
    public static void main(String[] args){
        //connect
        ConnectionString connString = new ConnectionString(
                "mongodb://root:123@172.19.240.108:27017/admin?w=majority"
        );
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .retryWrites(true)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        //get database
        MongoDatabase database = mongoClient.getDatabase("db1");
        //get collection
        MongoCollection<Document> collection = database.getCollection("col1");
        //insert document
        Document document = new Document();
        document.put("roambin/mongoutils/json", "abc");
        collection.insertOne(document);
        //select
        FindIterable<Document> filterIterator = collection.find();

        StringBuffer bf = new StringBuffer();
        filterIterator.forEach((Consumer<Document>)e -> bf.append(e.get("roambin/mongoutils/json")).append('\n'));
        database.drop();
        //close connect
        mongoClient.close();
        System.out.println(bf);
    }

}
