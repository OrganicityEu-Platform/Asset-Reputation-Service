package mongo;

import com.mongodb.*;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;


public class MongoDBItemWriter implements ItemWriter<Object>, InitializingBean, ChunkListener {


    private static final boolean DEFAULT_TRANSACTIONAL = true;


    private static final boolean DEFAULT_CHECK_WRITE_RESULT = true;


    protected Mongo mongo;

    protected String db;

    protected String collection;

    protected Converter<Object, DBObject> converter;

    protected WriteConcern writeConcern;

    protected boolean transactional = DEFAULT_TRANSACTIONAL;

    protected boolean checkWriteResult = DEFAULT_CHECK_WRITE_RESULT;

    private List<DBObject> dbObjectCache = null;

    private Throwable mongoDbFailure = null;

    public MongoDBItemWriter(String host, int port, String db, String collection) {
        mongo = new MongoClient(host, port);
        this.setDb(db);
        this.setCollection(collection);
    }
    // public item writer interface .........................................

    @Override
    public void write(List<? extends Object> items) throws Exception {
        final WriteConcern wc = writeConcern == null ? mongo.getWriteConcern() : writeConcern;

        if (transactional && TransactionSynchronizationManager.isActualTransactionActive()) {
            dbObjectCache = prepareDocuments(items);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {

                @Override
                public void afterCompletion(int status) {
                    try {
                        if (status == STATUS_COMMITTED) {
                            doInsert(db, collection, wc, dbObjectCache);
                        }
                    } catch (Throwable t) {
                        mongoDbFailure = t;
                    } finally {
                        dbObjectCache = null;
                    }
                }
            });
        } else {
            doInsert(db, collection, wc, prepareDocuments(items));
        }
    }

    // private methods .....................................................
    private List<DBObject> prepareDocuments(List<? extends Object> items) {
        final List<DBObject> docs = new ArrayList<DBObject>();

        if (items != null) {
            for (Object item : items) {
                if (item instanceof DBObject) {
                    docs.add((DBObject) item);
                } else if (converter != null) {
                    docs.add(converter.convert(item));
                } else {
                    throw new IllegalArgumentException("Cannot convert item to DBObject: " + item);
                }
            }
        }
        return docs;
    }

    // Setter ...............................................................

    public void setMongo(Mongo mongo) {
        this.mongo = mongo;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setConverter(Converter<Object, DBObject> converter) {
        this.converter = converter;
    }

    public void setWriteConcern(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }


    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    public void setCheckWriteResult(boolean checkWriteResult) {
        this.checkWriteResult = checkWriteResult;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(mongo, "A Mongo instance is required");
        Assert.hasText(db, "A database name is required");
        Assert.hasText(collection, "A collection name is required");
    }


    /**
     * Perform the insert operation on the collection.
     *
     * @param databaseName   Name of the database to use.
     * @param collectionName Name of the collection to use.
     * @param wc             WriteConcern.
     * @param docs           List of documents to insert.
     */
    protected void doInsert(String databaseName, String collectionName, WriteConcern wc, List<DBObject> docs) {
        for (DBObject o : docs) {
            DBObject rob = new BasicDBObject();
            rob.put("_id", o.get("_id"));
            mongo.getDB(databaseName).getCollection(collectionName).remove(rob, wc);
        }
        try {
            WriteResult wr = mongo.getDB(databaseName).getCollection(collectionName).insert(docs, WriteConcern.SAFE);
        } catch (MongoServerException e) {
            throw new MongoDBInsertFailedException(databaseName, collectionName, e.getMessage());
        }
    }


    @Override
    public void beforeChunk(ChunkContext context) {

    }

    @Override
    public void afterChunk(ChunkContext context) {
        try {
            if (mongoDbFailure != null) {
                if (mongoDbFailure instanceof MongoDBInsertFailedException) {
                    throw (MongoDBInsertFailedException) mongoDbFailure;
                } else {
                    throw new MongoDBInsertFailedException(db, collection, "Could not insert document/s into collection", mongoDbFailure);
                }
            }
        } finally {
            mongoDbFailure = null;
        }
    }

    @Override
    public void afterChunkError(ChunkContext context) {

    }
}