package edu.nju.bilianalysis.result;

import java.io.Serializable;
import java.util.*;

public class Cluster implements Serializable {
    public int cluster;
    public int view;
    public int barrage;
    public int like;
    public int coin;
    public int collect;
    public int share;
    public int comment;
    public Map<String, Integer> tag;
    public Cluster(){

    }
    public Cluster(int cluster){
        this.cluster = cluster;
        tag = new LinkedHashMap<>();
    }
    public void getSortTag(HashMap<String, Integer> map){
        ArrayList<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2)-> o2.getValue().compareTo(o1.getValue()));
        for(int i = 0; i < 15 && i < list.size(); i++){
            tag.put(list.get(i).getKey(), list.get(i).getValue());
        }
    }
}
