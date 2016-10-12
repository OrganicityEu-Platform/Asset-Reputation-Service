package operations;


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;

public class FuseStatisticObjectToReputationScore implements ItemProcessor<Object, Object> {
    @Override
    public Object process(Object item) throws Exception {
        DBObject item2 = (DBObject) item;
        String asset = ((BasicDBObject) item2.get("_id")).get("id").toString();
        Double reputation = -1.0;
        try {
            HashMap<String, Double> stats = BatchOperation.memory.getStatistics(asset);
            reputation = 0.0;
            for (String stat : stats.keySet()) {
                reputation += stats.get(stat);
            }
        } catch (Exception e) {
            reputation = -1.0;
        }
        item2.put("Reputation", reputation);
        item2.put("ReputationTime", System.currentTimeMillis()/1000);
        return item2;
    }
}