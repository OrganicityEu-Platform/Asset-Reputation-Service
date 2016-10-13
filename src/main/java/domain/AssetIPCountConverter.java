package domain;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;

public class AssetIPCountConverter implements Converter<DBObject, Statistic> {
    private static final Logger LOG = LoggerFactory.getLogger(AssetIPCountConverter.class);
    @Override
    public Statistic convert(DBObject item) {
        Statistic entry = new Statistic();
        try {
            entry.setAsset(item.get("_id").toString());
            entry.setStatName("TotalIPsCount");
            entry.setStatValue(Double.valueOf(item.get("count").toString()));
            LOG.info(entry.getAsset() + "," + entry.getStatName() + "," + entry.getStatValue());
        }catch (Exception e){
            LOG.debug(item.toString());
        }
            return entry;
    }
}