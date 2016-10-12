package domain;

import com.mongodb.DBObject;
import org.springframework.core.convert.converter.Converter;

public class AssetIPCountConverter implements Converter<DBObject, Statistic> {

    @Override
    public Statistic convert(DBObject item) {
        Statistic entry = new Statistic();
        entry.setAsset(item.get("_id").toString());
        entry.setStatName("TotalIPsCount");
        entry.setStatValue(Double.valueOf(item.get("count").toString()));
        return entry;
    }
}