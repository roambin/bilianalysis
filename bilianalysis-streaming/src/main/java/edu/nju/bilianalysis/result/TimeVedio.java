package edu.nju.bilianalysis.result;

import java.io.Serializable;
import java.util.ArrayList;

public class TimeVedio implements Serializable {
    public int count;
    public ArrayList<String> tabList;
    public TimeVedio(){

    }
    public TimeVedio(int count, ArrayList<String> tagList){
        this.count = count;
        this.tabList = tagList;
    }
}
