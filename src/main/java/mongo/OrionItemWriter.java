package mongo;

import com.mongodb.*;
import operations.BatchOperation;
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
            DBObject inputObject = (DBObject) o;
            Double rep = (Double) inputObject.get("Reputation");
            Long repTime = (Long) inputObject.get("ReputationTime");


           // BasicDBObject updatedObject = new BasicDBObject();
           // updatedObject.append("_id", inputObject.get("_id"));
            BasicDBList att = (BasicDBList) inputObject.get("attrNames");
            if (att.indexOf("reputation") == -1) {
                att.add("reputation");
               // updatedObject.append("attrNames", att);
            }
            BasicDBObject att2 = (BasicDBObject) inputObject.get("attrs");
            BasicDBObject r = (BasicDBObject) att2.get("reputation");
            if (r!=null && r.get("value") != null) {
                Double value = (Double) r.get("value");
                if (value.equals(rep))
                    LOG.info("Reputation No Update: " + ((BasicDBObject)inputObject.get("_id")).get("id").toString() + "  Value:" + rep);
                    continue;
            }

            BasicDBObject repAttr = new BasicDBObject();
            if (r != null) {
                repAttr.put("creDate", r.get("creDate"));
            } else {
                repAttr.put("creDate", System.currentTimeMillis() / 1000);
            }

            repAttr.put("value", (rep == null) ? -1 : rep);
            repAttr.put("type", "urn:oc:attributeType:reputation");
            repAttr.put("modDate", repTime);
            att2.append("reputation", repAttr);
            //inputObject.append("attrs.reputation", repAttr);
            BasicDBObject command = new BasicDBObject();
            command.put("$set", inputObject);
            try {
               // WriteResult wr = mongo.getDB(db).getCollection(collection).update(inputObject, command, true, true, WriteConcern.NORMAL);
                WriteResult wr = mongo.getDB(db).getCollection(collection).save(inputObject);
                BatchOperation.memory.updated();
                LOG.info("Reputation Update" + inputObject.get("_id").toString() + "  Value:" + rep);
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