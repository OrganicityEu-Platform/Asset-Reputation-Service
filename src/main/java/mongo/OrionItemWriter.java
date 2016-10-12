package mongo;

import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.List;


public class OrionItemWriter implements ItemWriter<Object>, InitializingBean, ChunkListener {

    private static final Logger LOG = LoggerFactory.getLogger(OrionItemWriter.class);

    protected Mongo mongo;

    protected String db;

    protected String collection;

    public OrionItemWriter(String host, int port, String db, String collection) {

        this.db = db;
        this.collection = collection;
        mongo = new MongoClient(host, port);
    }

    @Override
    public void write(List<? extends Object> items) throws Exception {

        for (Object o : items) {
            DBObject dbo = (DBObject) o;
            Double rep = (Double) dbo.get("Reputation");
            Long repTime = (Long) dbo.get("ReputationTime");

            BasicDBObject query = new BasicDBObject();
            query.append("_id", dbo.get("_id"));
            BasicDBObject update = new BasicDBObject();
            update.append("_id", dbo.get("_id"));
            BasicDBList att = (BasicDBList) dbo.get("attrNames");
            if (att.indexOf("reputation") == -1) {
                att.add("reputation");
                update.append("attrNames", att);
            }
            BasicDBObject att2 = (BasicDBObject) dbo.get("attrs");
            BasicDBObject r = (BasicDBObject) att2.get("reputation");
            BasicDBObject repAttr = new BasicDBObject();
            if (r.get("value") != null) {
                Double value = (Double) r.get("value");
                if (value.equals(rep))
                    continue;
            }
            if (r != null) {
                repAttr.put("creDate", r.get("creDate"));
            } else {
                repAttr.put("creDate", System.currentTimeMillis() / 1000);
            }

            repAttr.put("value", (rep == null) ? -1 : rep);
            repAttr.put("type", "urn:oc:attributeType:reputation");
            repAttr.put("modDate", repTime);
            update.append("attrs.reputation", repAttr);
            BasicDBObject command = new BasicDBObject();
            command.put("$set", update);
            try {
                WriteResult wr = mongo.getDB(db).getCollection(collection).update(query, command, false, false, WriteConcern.SAFE);
                LOG.debug("Reputation Update" + dbo.get("_id").toString() + "  Value:" + rep);
            } catch (MongoServerException e) {
                throw new MongoDBInsertFailedException(db, collection, e.getMessage());
            }

        }


    }


    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(db, "dbis required");
        Assert.notNull(collection, "collection is required");
    }

    @Override
    public void beforeChunk(ChunkContext context) {

    }

    @Override
    public void afterChunk(ChunkContext context) {

    }

    @Override
    public void afterChunkError(ChunkContext context) {

    }

}