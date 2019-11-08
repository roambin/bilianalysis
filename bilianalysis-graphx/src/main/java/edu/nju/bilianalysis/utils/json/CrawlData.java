package edu.nju.bilianalysis.utils.json;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CrawlData implements Serializable {
    public int av_num;
    public String title;
    public int view = 0;
    public int barrage = 0;
    public int like = 0;
    public int coin = 0;
    public int collect = 0;
    public int share = 0;
    public int comment = 0;
    public String time;
    public List<String> tag;
    public CrawlData(){

    }
    public static CrawlData sum(CrawlData... crawlDatas){
        CrawlData sum = new CrawlData();
        for(CrawlData obj: crawlDatas){
            sum.view += obj.view;
            sum.barrage += obj.barrage;
            sum.like += obj.like;
            sum.coin += obj.coin;
            sum.collect += obj.collect;
            sum.share += obj.share;
            sum.comment += obj.comment;
        }
        return sum;
    }
    public void add(CrawlData... crawlDatas){
        for(CrawlData obj: crawlDatas){
            this.view += obj.view;
            this.barrage += obj.barrage;
            this.like += obj.like;
            this.coin += obj.coin;
            this.collect += obj.collect;
            this.share += obj.share;
            this.comment += obj.comment;
        }
    }
}
