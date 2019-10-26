package edu.nju.bilianalysis.utils.json;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import edu.nju.bilianalysis.conf.MyConf1;
import edu.nju.bilianalysis.result.Cluster;
import edu.nju.bilianalysis.utils.mongodb.MongoUtils;
import org.bson.Document;

import java.util.LinkedHashMap;

public class MyJsonCluster {
    public static JSONObject objectToJson(Cluster obj) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("view", obj.view);
        jsonObject.put("barrage", obj.barrage);
        jsonObject.put("like", obj.like);
        jsonObject.put("coin", obj.coin);
        jsonObject.put("collect", obj.collect);
        jsonObject.put("share", obj.share);
        jsonObject.put("comment", obj.comment);
        jsonObject.put("tag", obj.tag);
        return jsonObject;
    }

    public static Cluster jsonToObject(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        return jsonToObject(jsonObject);
    }
    public static Cluster jsonToObject(JSONObject jsonObject) {
        Cluster obj = new Cluster();
        obj.view = jsonObject.getIntValue("view");
        obj.barrage = jsonObject.getIntValue("barrage");
        obj.like = jsonObject.getIntValue("like");
        obj.coin = jsonObject.getIntValue("coin");
        obj.collect = jsonObject.getIntValue("collect");
        obj.share = jsonObject.getIntValue("share");
        obj.comment = jsonObject.getIntValue("comment");
        obj.tag = (LinkedHashMap<String, Integer>)jsonObject.get("tag");
        return obj;
    }
    public static void mongoInsertCluster(Cluster s, String collectionName){
        MongoClient client = MongoUtils.getClient(MyConf1.MONGO_CONNECTION);
        MongoCollection<Document> collection = client.getDatabase("mllib").getCollection(collectionName);
        collection.insertOne(new Document(MyJsonCluster.objectToJson(s)));
        client.close();
    }
    public static void main(String[] args){
        Cluster obj = new Cluster(1);
        obj.view = 1;
        obj.barrage = 1;
        obj.like = 1;
        obj.coin = 1;
        obj.collect = 1;
        obj.share = 1;
        obj.comment = 1;
        obj.tag.put("a", 1);
        obj.tag.put("b", 2);
        mongoInsertCluster(obj, "test");
    }
}
