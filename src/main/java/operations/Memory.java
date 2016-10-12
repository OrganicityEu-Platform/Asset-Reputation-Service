package operations;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.HashMap;

/**
 * Created by etheodor on 10/06/2016.
 */

public class Memory {

    HashMap<String, HashMap<String, Double>> statistics = new HashMap<>();


    void clear() {
        statistics.clear();
    }

    void pushStatistic(String asset, String statName, Double statValue) {
        if (statistics.get(asset) == null) {
            HashMap<String, Double> stats = new HashMap<>();
            statistics.put(asset, stats);
        }
        statistics.get(asset).put(statName, statValue);
    }

    Double getStatistic(String asset, String statName) throws Exception {
        if (statistics.get(asset) == null) {
            throw new Exception("No Asset in the memory");
        }
        return statistics.get(asset).get(statName);
    }

    HashMap<String, Double> getStatistics(String asset) throws Exception {
        if (statistics.get(asset) == null) {
            throw new Exception("No Asset in the memory");
        }
        return statistics.get(asset);
    }
    DBObject getStatisticsDBObject(String asset) throws Exception {
        DBObject object=new BasicDBObject();

        HashMap<String, Double> h=statistics.get(asset);
        if (h==null) throw new Exception("No Statistics for this asset");
        object.put("_id",asset);
        for(String stat : h.keySet()){
            object.put(stat,h.get(stat));
        }
        return object;
    }
}
