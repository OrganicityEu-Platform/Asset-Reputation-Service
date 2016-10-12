package mongo;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.Iterator;
import java.util.List;


public class MongoDBItemReader
        extends AbstractItemCountingItemStreamItemReader<Object>
        implements InitializingBean {

    /**
     * A RuntimeException w/ this msg signals: no more documents can be read.
     */
    private static final String NO_MORE = "no more";
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBItemReader.class);


    // configurable attributes ......................................

    /**
     * MongoDB connection pool.
     */
    protected Mongo mongo;

    /**
     * Name of the database to read from.
     */
    protected String db;

    /**
     * Name of the collection to read from.
     */
    protected String collection;

    /**
     * Query in JSON notation, e.g. <code>{a: 1, b: 2}</code> (optional).
     * <p/>
     * If no query is given, the whole collection is read.
     */
    protected String query;
    protected String project;
    protected String group;

    /**
     * JSON document that filters the returned fields (optinal).
     */
    protected String keys;

    /**
     * Custom converter to map {@DBObject}s to Java POJOs (optional).
     */
    protected Converter<DBObject, ?> converter;

    /**
     * Number of documents to read in one batch (optional).
     *
     * @see DBCursor#batchSize(int)
     */
    protected int batchSize;

    /**
     * Sort criteria in JSON notation,e.g. <code>{a: -1, b: 1}</code> (optional).
     *
     * @see DBCursor#sort(DBObject)
     */
    protected String sort;

    /**
     * Use a snapshot query (optional). Default is <code>false</code>.
     *
     * @see DBCursor#snapshot()
     */
    protected boolean snapshot;

    /**
     * Limit the amount of read documents (optional).
     *
     * @see DBCursor#limit(int)
     */
    protected int limit;

    /**
     * Skip the first n document in the query result (optional).
     *
     * @see DBCursor#skip(int)
     */
    protected int skip;


    // internally used attributes ......................................

    /**
     * Cursor pointing to the current document.
     */
    protected DBCursor cursor;
    protected Iterator<DBObject> cursor2;


    // public item reader interface .........................................

    public MongoDBItemReader(String host, int port, String db, String collection, Converter converter, String query, String project, String group) {
        setName(ClassUtils.getShortName(MongoDBItemReader.class));
        mongo = new MongoClient(host, port);
        this.setDb(db);
        this.setCollection(collection);
        this.converter = converter;
        this.query = query;
        this.group = group;
        this.project = project;
    }

    private static DBObject parseDocument(String json) {
        try {
            return (DBObject) JSON.parse(json);
        } catch (JSONParseException e) {
            throw new IllegalArgumentException("Not a valid JSON document: " + json, e);
        }
    }

    @Override
    protected void jumpToItem(int itemIndex) throws Exception {
        if (itemIndex < 0) {
            throw new IllegalArgumentException("Index must not be negative: " + itemIndex);
        }
        if (cursor!=null) cursor.skip(itemIndex + skip);
    }

    @Override
    protected void doOpen() throws Exception {
        // negative skip value are not supported
        if (skip < 0) {
            throw new IllegalArgumentException("Negative skip values are not supported: " + skip);
        }

        LOG.debug("doOpen");

        // do NOT read from a db that does not exist
        if (!dbExists()) {
            throw new IllegalArgumentException("No such database: " + db);
        }

        final DB mongoDB = mongo.getDB(db);
        // do NOT read from collections that do not exist
        if (!mongoDB.collectionExists(collection)) {
            throw new IllegalArgumentException("No such collection: " + collection);
        }

        if (query != null) {
            cursor = createCursor(mongoDB.getCollection(collection));
            cursor2 = null;
        } else {
            Iterable<DBObject> results;
            if (group != null)
                results = mongoDB.getCollection(collection).aggregate(parseDocument(project), parseDocument(group)).results();
            else
                results = mongoDB.getCollection(collection).aggregate(parseDocument(project)).results();
            cursor2 = results.iterator();
        }

    }

    @Override
    public Object doRead() throws Exception {
        if (cursor2 ==null && cursor==null) doOpen();
        try {
            if (cursor2 != null) {
                return converter != null ? converter.convert(cursor2.next()) : cursor2.next();
            }
            return converter != null ? converter.convert(cursor.next()) : cursor.next();
        } catch (RuntimeException e) {
            if (NO_MORE.equals(e.getMessage())) {
                return null;
            } else {
                throw e;
            }

        }
    }


    // Internal methods .....................................................

    @Override
    protected void doClose() throws Exception {
        if (cursor != null) {
            cursor.close();
        }
        if (cursor2 != null) {
            cursor2=null;
        }
    }

    private boolean dbExists() {
        List<String> dbNames = mongo.getDatabaseNames();

        return dbNames != null && dbNames.contains(db);
    }

    private DBCursor createCursor(DBCollection coll) {
        DBCursor crsr;
        DBObject ref = null;
        DBObject keysDoc = null;

        if (StringUtils.hasText(query)) {
            ref = parseDocument(query);
        }

        if (StringUtils.hasText(keys)) {
            keysDoc = parseDocument(keys);
        }

        crsr = coll.find(ref, keysDoc);
        if (StringUtils.hasText(sort)) {
            crsr = crsr.sort(parseDocument(sort));
        }
        if (batchSize != 0) {
            crsr = crsr.batchSize(batchSize);
        }
        if (snapshot) {
            crsr = crsr.snapshot();
        }
        if (limit != 0) {
            crsr = crsr.limit(limit);
        }
        if (skip > 0) {
            crsr = crsr.skip(skip);
        }
        return crsr;
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


    public void setQuery(String query) {
        this.query = query;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public void setConverter(Converter<DBObject, ?> converter) {
        this.converter = converter;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    public void setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(mongo, "A mongo instance is required");
        Assert.hasText(db, "A database name is required");
        Assert.hasText(collection, "A collection name is required");
    }

}