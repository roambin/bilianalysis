package edu.nju.bilianalysis.utils.json;

import java.io.Serializable;

public class VertexValue implements Serializable {
    public int view = 0;
    public int barrage = 0;
    public int like = 0;
    public int coin = 0;
    public int collect = 0;
    public int share = 0;
    public int comment = 0;
    public String tag = null;
    public int count = 1;
    public VertexValue(){

    }
    public VertexValue(String tag){
        this.tag = tag;
    }
    public VertexValue(String tag, CrawlData crawlData){
        this.tag = tag;
        this.view += crawlData.view;
        this.barrage += crawlData.barrage;
        this.like += crawlData.like;
        this.coin += crawlData.coin;
        this.collect += crawlData.collect;
        this.share += crawlData.share;
        this.comment += crawlData.comment;
    }
    public VertexValue add(VertexValue vertexValue){
        this.view += vertexValue.view;
        this.barrage += vertexValue.barrage;
        this.like += vertexValue.like;
        this.coin += vertexValue.coin;
        this.collect += vertexValue.collect;
        this.share += vertexValue.share;
        this.comment += vertexValue.comment;
        this.count += vertexValue.count;
        return this;
    }
}
