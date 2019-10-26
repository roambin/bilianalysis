import java.io.Serializable;
import java.util.List;

public class CrawlData implements Serializable {
    public int av_num;
    public String title;
    public int view;
    public int barrage;
    public int like;
    public int coin;
    public int collect;
    public int share;
    public int comment;
    public String time;
    public List<String> tag;
    public CrawlData(){

    }
}
