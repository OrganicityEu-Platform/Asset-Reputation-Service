package operations;

import domain.Statistic;
import org.springframework.batch.item.ItemProcessor;

public class FuseStatisticObjectToDBObject implements ItemProcessor<Object, Object> {
    @Override
    public Object process(Object item) throws Exception {
        Statistic item2 = (Statistic) item;
        BatchOperation.memory.pushStatistic(item2.getAsset(), item2.getStatName(), item2.getStatValue());
        return BatchOperation.memory.getStatisticsDBObject(item2.getAsset());
    }
}