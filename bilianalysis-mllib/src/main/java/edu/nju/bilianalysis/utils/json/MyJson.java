package edu.nju.bilianalysis.utils.json;

import com.alibaba.fastjson.JSONObject;

public class MyJson {
    public static JSONObject objectToJson(CrawlData crawlData) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("av_num", crawlData.av_num);
        jsonObject.put("title", crawlData.title);
        jsonObject.put("view", crawlData.view);
        jsonObject.put("barrage", crawlData.barrage);
        jsonObject.put("like", crawlData.like);
        jsonObject.put("coin", crawlData.coin);
        jsonObject.put("collect", crawlData.collect);
        jsonObject.put("share", crawlData.share);
        jsonObject.put("comment", crawlData.comment);
        jsonObject.put("time", crawlData.time);
        jsonObject.put("tag", crawlData.tag);
        return jsonObject;
    }

    public static CrawlData jsonToObject(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        return jsonToObject(jsonObject);
    }
    public static CrawlData jsonToObject(JSONObject jsonObject) {
        CrawlData crawlData = new CrawlData();
        crawlData.av_num = jsonObject.getIntValue("av_num");
        crawlData.title = jsonObject.getString("title");
        crawlData.view = jsonObject.getIntValue("view");
        crawlData.barrage = jsonObject.getIntValue("barrage");
        crawlData.like = jsonObject.getIntValue("like");
        crawlData.coin = jsonObject.getIntValue("coin");
        crawlData.collect = jsonObject.getIntValue("collect");
        crawlData.share = jsonObject.getIntValue("share");
        crawlData.comment = jsonObject.getIntValue("comment");
        crawlData.time = jsonObject.getString("time");
        crawlData.tag = jsonObject.getJSONArray("tag").toJavaList(String.class);
        return crawlData;
    }
}
