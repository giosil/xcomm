package org.dew.xcomm.nosql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.bson.Document;
import org.bson.types.ObjectId;

import org.dew.xcomm.nosql.json.JSON;
import org.dew.xcomm.nosql.util.WUtil;

import org.dew.xcomm.util.BEConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

@SuppressWarnings({"rawtypes", "unchecked"})
public
class NoSQLMongoDB3 implements INoSQLDB
{
  protected static String logprefix = NoSQLMongoDB3.class.getSimpleName() + ".";
  protected static Map<String, MongoClient> mapMDBClient = new HashMap<String, MongoClient> ();
  protected static Set<String> indexesCreated;
  
  protected MongoDatabase db;
  protected boolean debug = false;
  protected int defLimit  = 10000;
  
  public NoSQLMongoDB3()
    throws Exception
  {
    String dbname = BEConfig.getDefaultDbName();
    this.db = getMongoClient().getDatabase(dbname);
  }
  
  public NoSQLMongoDB3(String dbname)
    throws Exception
  {
    if(dbname == null || dbname.length() == 0) dbname = BEConfig.getDefaultDbName();
    this.db = getMongoClient().getDatabase(dbname);
  }
  
  public NoSQLMongoDB3(MongoDatabase db)
    throws Exception
  {
    if(db != null) {
      this.db = db;
    }
    else {
      String dbname = BEConfig.getDefaultDbName();
      this.db = getMongoClient().getDatabase(dbname);
    }
  }
  
  @Override
  public
  void setDebug(boolean debug)
  {
    this.debug = debug;
  }
  
  @Override
  public
  boolean isDebug()
  {
    return debug;
  }
  
  @Override
  public
  Map<String,Object> getInfo()
    throws Exception
  {
    if(debug) System.out.println(logprefix + "getInfo()...");
    
    Map<String,Object> mapResult = new HashMap<String,Object> (2);
    
    Document buildinfo = db.runCommand(new Document("buildInfo", 1));
    if(buildinfo != null) {
      mapResult.put("name",    "mongodb");
      mapResult.put("version", buildinfo.get("version"));
    }
    
    if(debug) System.out.println(logprefix + "getInfo() -> " + mapResult);
    return mapResult;
  }
  
  @Override
  public
  List<String> getCollections()
    throws Exception
  {
    if(debug) System.out.println(logprefix + "getCollections()...");
    
    MongoIterable<String> collectionNames = db.listCollectionNames();
    
    List<String> result = new ArrayList<String> ();
    
    for(String collectionName : collectionNames) {
      result.add(collectionName);
    }
    
    Collections.sort(result);
    
    if(debug) System.out.println(logprefix + "getCollections() -> " + result);
    return result;
  }
  
  @Override
  public
  boolean drop(String collection) 
    throws Exception
  {
    if(debug) System.out.println(logprefix + "drop(" + collection + ")...");
    boolean result = false;
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    mongoCollection.drop();
    
    result = true;
    
    if(debug) System.out.println(logprefix + "drop(" + collection + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String,?> mapData)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    Document document = toDocument(mapData);
    
    if(debug) System.out.println(logprefix + "insert " + collection + ".insertOne(" + document + ")...");
    
    mongoCollection.insertOne(document);
    
    String result = getId(document);
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String,?> mapData, boolean refresh)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    Document document = toDocument(mapData);
    
    if(debug) System.out.println(logprefix + "insert " + collection + ".insertOne(" + document + ")...");
    
    mongoCollection.insertOne(document);
    
    String result = getId(document);
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int bulkIns(String collection, List<Map<String,?>> listData)
    throws Exception
  {
    if(debug) {
      if(listData != null) {
        System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents)...");
      }
      else {
        System.out.println(logprefix + "bulkIns(" + collection + ", null)...");
      }
    }
    if(listData == null || listData.size() == 0) {
      if(listData != null) {
        System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> 0");
      }
      else {
        System.out.println(logprefix + "bulkIns(" + collection + ", null) -> 0");
      }
      return 0;
    }
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    List<InsertOneModel<Document>> listOfWriteModel = new ArrayList<InsertOneModel<Document>> (listData.size());
    for(int i = 0; i < listData.size(); i++) {
      Map<String,?> mapData = listData.get(i);
      Document document = toDocument(mapData);
      if(debug) System.out.println(logprefix + "bulkIns listOfWriteModel.add(new InsertOneModel(" + document + "))...");
      listOfWriteModel.add(new InsertOneModel<Document> (document));
    }
    
    if(debug) System.out.println(logprefix + "bulkIns " + collection + ".bulkWrite(listOfWriteModel)...");
    BulkWriteResult bulkWriteResult = mongoCollection.bulkWrite(listOfWriteModel);
    
    int result = bulkWriteResult.getInsertedCount();
    System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> " + result);
    return result;
  }
  
  @Override
  public
  boolean replace(String collection, Map<String,?> mapData, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "replace(" + collection + "," + mapData + "," + id + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    Document document = toDocument(mapData);
    
    BasicDBObject filter = buildQueryFilter(id);
    
    if(debug) System.out.println(logprefix + "replace " + collection + ".replaceOne(" + filter + "," + document +")...");
    
    UpdateResult ur = mongoCollection.replaceOne(filter, document);
    
    boolean result = ur.getModifiedCount() > 0;
    if(debug) System.out.println(logprefix + "replace(" + collection + "," + mapData + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int update(String collection, Map<String,?> mapData, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ")...");
    
    if(mapData == null || mapData.isEmpty()) {
      if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> 0");
      return 0;
    }
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    Document document = toDocument(mapData);
    
    BasicDBObject filter = buildQueryFilter(id);
    
    BasicDBObject dbset = new BasicDBObject("$set", document);
    
    if(debug) System.out.println(logprefix + "update " + collection + ".updateOne(" + filter + "," + dbset +")...");
    
    UpdateResult ur = mongoCollection.updateOne(filter, dbset);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int update(String collection, Map<String,?> mapData, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ")...");
    
    if(mapData == null || mapData.isEmpty()) {
      if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> 0");
      return 0;
    }
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    Document document = toDocument(mapData);
    
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    BasicDBObject dbset = new BasicDBObject("$set", document);
    
    if(debug) System.out.println(logprefix + "update " + collection + ".updateMany(" + filter + "," + dbset + ")...");
    
    UpdateResult ur = mongoCollection.updateMany(filter, dbset);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String upsert(String collection, Map<String,?> mapData, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    Document document = toDocument(mapData);
    
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    BasicDBObject dbset = new BasicDBObject("$set", document);
    
    if(debug) System.out.println(logprefix + "update " + collection + ".updateOne(" + filter + "," + dbset + ",new UpdateOptions().upsert(true))...");
    
    UpdateResult ur = mongoCollection.updateOne(filter, dbset, new UpdateOptions().upsert(true));
    
    Object upsertedId = ur.getUpsertedId();
    String id = null;
    if(upsertedId instanceof ObjectId) {
      id = ((ObjectId) upsertedId).toHexString();
    }
    else
    if(upsertedId != null) {
      id = upsertedId.toString();
    }
    if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ") -> " + id);
    return id;
  }
  
  @Override
  public
  int unset(String collection, String fields, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ")...");
    
    if(fields == null || fields.length() == 0) return 0;
    
    Document document = new Document();
    int iIndexOf  = 0;
    int iBegin    = 0;
    iIndexOf      = fields.indexOf(',');
    while(iIndexOf >= 0) {
      document.append(fields.substring(iBegin,iIndexOf),"");
      iIndexOf = fields.indexOf(',', iBegin=iIndexOf+1);
    }
    document.append(fields.substring(iBegin),"");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(id);
    
    BasicDBObject dbunset = new BasicDBObject("$unset", document);
    
    if(debug) System.out.println(logprefix + "unset " + collection + ".updateOne(" + filter + "," + dbunset +")...");
    
    UpdateResult ur = mongoCollection.updateOne(filter, dbunset);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, String id, String field, Number value)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(id);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field, value));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".updateOne(" + filter + "," + dbinc + ")...");
    
    UpdateResult ur = mongoCollection.updateOne(filter, dbinc);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, String id, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(id);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field1, value1).append(field2, value2));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".updateOne(" + filter + "," + dbinc + ")...");
    
    UpdateResult ur = mongoCollection.updateOne(filter, dbinc);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, Map<String,?> mapFilter, String field, Number value)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field, value));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".updateMany(" + filter + "," + dbinc + ")...");
    
    UpdateResult ur = mongoCollection.updateMany(filter, dbinc);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, Map<String,?> mapFilter, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field1, value1).append(field2, value2));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".updateMany(" + filter + "," + dbinc + ")...");
    
    UpdateResult ur = mongoCollection.updateMany(filter, dbinc);
    
    int result = (int) ur.getModifiedCount();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int delete(String collection, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ")...");
    
    if(collection == null || id == null) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> 0");
      return 0;
    }
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    if(id.equals("!")) {
      if(debug) System.out.println(logprefix + "delete " + collection + ".drop()");
      mongoCollection.drop();
      return 1;
    }
    
    BasicDBObject filter = buildQueryFilter(id);
    
    if(debug) System.out.println(logprefix + "delete " + collection + ".deleteOne(" + filter + ")");
    
    DeleteResult dr = mongoCollection.deleteOne(filter);
    
    int result = (int) dr.getDeletedCount();
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int delete(String collection, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    if(debug) System.out.println(logprefix + "delete " + collection + ".remove(" + filter + ")");
    
    DeleteResult dr = mongoCollection.deleteMany(filter);
    
    int result = (int) dr.getDeletedCount();
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\")...");
    
    Document projection = null;
    String[] toexcl = null;
    String[] asproj = null;
    if(mapFilter != null) {
      toexcl = WUtil.toArrayOfString(mapFilter.get(FILTER_EXCLUDE), false);
      if(fields == null || fields.length() == 0 || fields.equals("*")) {
        asproj = WUtil.toArrayOfString(mapFilter.get(FILTER_FIELDS), false);
      }
      else {
        asproj = WUtil.toArrayOfString(fields, false);
      }
      if(asproj != null && asproj.length > 0) {
        projection = new Document();
        for(int i = 0; i < asproj.length; i++) {
          projection.put(asproj[i], 1);
        }
      }
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    MongoCursor<Document> mongoCursor = null;
    try {
      MongoCollection<Document> mongoCollection = db.getCollection(collection);
      
      BasicDBObject filter = buildQueryFilter(mapFilter);
      
      if(debug) System.out.println(logprefix + "find " + collection + ".find(" + filter + ").limit(" + defLimit + ")");
      
      if(projection != null) {
        mongoCursor = mongoCollection.find(filter).limit(defLimit).projection(projection).iterator();
      }
      else {
        mongoCursor = mongoCollection.find(filter).limit(defLimit).iterator();
      }
      
      while(mongoCursor.hasNext()) {
        Document document = mongoCursor.next();
        if(toexcl != null) {
          for(int i = 0; i < toexcl.length; i++) {
            document.remove(toexcl[i]);
          }
        }
        Object _id = document.get("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        if(_id != null) document.put("_id", _id);
        listResult.add(document);
      }
    }
    finally {
      if(mongoCursor != null) try{ mongoCursor.close(); } catch(Exception ex) {}
    }
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields, String orderBy, int limit)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ")...");
    
    Document projection = null;
    String[] toexcl = null;
    String[] asproj = null;
    if(mapFilter != null) {
      toexcl = WUtil.toArrayOfString(mapFilter.get(FILTER_EXCLUDE), false);
      if(fields == null || fields.length() == 0 || fields.equals("*")) {
        asproj = WUtil.toArrayOfString(mapFilter.get(FILTER_FIELDS), false);
      }
      else {
        asproj = WUtil.toArrayOfString(fields, false);
      }
      if(asproj != null && asproj.length > 0) {
        projection = new Document();
        for(int i = 0; i < asproj.length; i++) {
          projection.put(asproj[i], 1);
        }
      }
    }
    if(limit < 1) limit = defLimit;
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    MongoCursor<Document> mongoCursor = null;
    try {
      MongoCollection<Document> mongoCollection = db.getCollection(collection);
      
      BasicDBObject filter = buildQueryFilter(mapFilter);
      
      if(orderBy != null && orderBy.length() > 0) {
        BasicDBObject dbsort = new BasicDBObject();
        int iIndexOf = 0;
        int iBegin   = 0;
        iIndexOf     = orderBy.indexOf(',');
        while(iIndexOf >= 0) {
          String sOrderClause = orderBy.substring(iBegin, iIndexOf).trim();
          dbsort.append(getOrderField(sOrderClause), getOrderType(sOrderClause));
          iBegin   = iIndexOf + 1;
          iIndexOf = orderBy.indexOf(',', iBegin);
        }
        String sOrderClause = orderBy.substring(iBegin).trim();
        dbsort.append(getOrderField(sOrderClause), getOrderType(sOrderClause));
        if(debug) System.out.println(logprefix + "find " + collection + ".find(" + filter + ").sort(" + dbsort + ").limit(" + limit + ")");
        if(projection != null) {
          mongoCursor = mongoCollection.find(filter).sort(dbsort).limit(limit).projection(projection).iterator();
        }
        else {
          mongoCursor = mongoCollection.find(filter).sort(dbsort).limit(limit).iterator();
        }
      }
      else {
        if(debug) System.out.println(logprefix + "find " + collection + ".find(" + filter + ").limit(" + limit + ")");
        if(projection != null) {
          mongoCursor = mongoCollection.find(filter).limit(limit).projection(projection).iterator();
        }
        else {
          mongoCursor = mongoCollection.find(filter).limit(limit).iterator();
        }
      }
      while(mongoCursor.hasNext()) {
        Document document = mongoCursor.next();
        if(toexcl != null) {
          for(int i = 0; i < toexcl.length; i++) {
            document.remove(toexcl[i]);
          }
        }
        Object _id = document.get("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        if(_id != null) document.put("_id", _id);
        listResult.add(document);
      }
    }
    finally {
      if(mongoCursor != null) try{ mongoCursor.close(); } catch(Exception ex) {}
    }
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String,Object>> search(String collection, String field, String text)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "search(" + collection + "," + field + "," + text + ")...");
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    MongoCursor<Document> mongoCursor = null;
    try {
      MongoCollection<Document> mongoCollection = db.getCollection(collection);
      
      if(indexesCreated == null) indexesCreated = new HashSet<String> ();
      if(!indexesCreated.contains(collection + "." + field)) {
        if(debug) System.out.println(logprefix + "search " + collection + ".createIndex({\"" + field + "\":\"text\"})...");
        mongoCollection.createIndex(new Document(field, "text"));
        indexesCreated.add(collection + "." + field);
      }
      
      BasicDBObject filter = new BasicDBObject("$text", new BasicDBObject("$search", text));
      
      if(debug) System.out.println(logprefix + "search " + collection + ".find(" + filter + ").limit(" + defLimit + ")");
      
      mongoCursor = mongoCollection.find(filter).limit(defLimit).iterator();
      
      while(mongoCursor.hasNext()) {
        Document document = mongoCursor.next();
        Object _id = document.get("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        if(_id != null) document.put("_id", _id);
        listResult.add(document);
      }
    }
    finally {
      if(mongoCursor != null) try{ mongoCursor.close(); } catch(Exception ex) {}
    }
    if(debug) System.out.println(logprefix + "search(" + collection + "," + field + "," + text + ") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String,Object>> group(String collection, Map<String,?> mapFilter, String field, String groupFunction)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\")...");
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    MongoCursor<Document> mongoCursor = null;
    try {
      MongoCollection<Document> mongoCollection = db.getCollection(collection);
      
      String alias = "";
      String funct = "";
      Object argum = "";
      if(groupFunction == null || groupFunction.length() == 0 || groupFunction.startsWith("c") || groupFunction.startsWith("C")) {
        alias = "count";
        funct = "$sum";
        argum = new Integer(1);
      }
      else {
        int iSep = groupFunction.indexOf('(');
        if(iSep > 0) {
          alias = groupFunction.substring(0,iSep);
          funct = "$" + groupFunction.substring(0,iSep).toLowerCase();
          argum = "$" + groupFunction.substring(iSep+1,groupFunction.length()-1);
        }
        else {
          alias = groupFunction;
          funct = "$" + groupFunction.toLowerCase();
          argum = new Integer(1);
        }
      }
      
      Document group = new Document("_id", "$" + field);
      group.put(alias, new Document(funct, argum));
      
      List<Document> pipeline = new ArrayList<Document>(2);
      pipeline.add(new Document("$match", buildQueryFilter(mapFilter)));
      pipeline.add(new Document("$group", group));
      
      if(debug) System.out.println(logprefix + "group " + collection + ".aggregate(" + pipeline + ").iterator()...");
      
      mongoCursor = mongoCollection.aggregate(pipeline).iterator();
      while(mongoCursor.hasNext()) {
        Document document = mongoCursor.next();
        Object _id = document.remove("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        if(_id != null) document.put(field, _id);
        Object value = document.remove(alias);
        if(value != null) document.put("value", value);
        listResult.add(document);
      }
    }
    finally {
      if(mongoCursor != null) try{ mongoCursor.close(); } catch(Exception ex) {}
    }
    if(debug) System.out.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  Map read(String collection, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ")...");
    
    if (id == null) {
      return null;
    }
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(id);
    
    if(debug) System.out.println(logprefix + "read " + collection + ".find(" + filter + ").first()");
    
    Document mapResult = mongoCollection.find(filter).first();
    
    if(mapResult == null) {
      if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> null");
      return null;
    }
    
    Object _id = mapResult.get("_id");
    if(_id instanceof ObjectId) {
      mapResult.put("_id",((ObjectId) _id).toHexString());
    }
    else
    if(_id != null) {
      mapResult.put("_id", _id.toString());
    }
    
    Iterator<Map.Entry<String,Object>> iterator = mapResult.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<String,Object> entry = iterator.next();
      Object value = entry.getValue();
      if(value instanceof org.bson.types.Binary) {
        org.bson.types.Binary bynary = (org.bson.types.Binary) value;
        entry.setValue(bynary.getData());
      }
    }
    
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> " + mapResult);
    return mapResult;
  }
  
  @Override
  public
  int count(String collection, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ")...");
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    if(debug) System.out.println(logprefix + "count " + collection + ".count(" + filter + ")");
    
    int result = (int) mongoCollection.count(filter);
    
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  boolean createIndex(String collection, String field, int type)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ")...");
    boolean result = false;
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    if(indexesCreated == null) indexesCreated = new HashSet<String> ();
    if(!indexesCreated.contains(collection + "." + field)) {
      if(debug) System.out.println(logprefix + "createIndex " + collection + ".createIndex({\"" + field + "\":" + type + "})...");
      mongoCollection.createIndex(new Document(field, type));
      indexesCreated.add(collection + "." + field);
      result = true;
    }
    
    if(debug) System.out.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ") -> " + result);
    return result;
  }
  
  @Override
  public
  List<Map<String,Object>> listIndexes(String collection) 
    throws Exception
  {
    if(debug) System.out.println(logprefix + "listIndexes(" + collection + ")...");
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>>();
    
    MongoCollection<Document> mongoCollection = db.getCollection(collection);
    
    for(Document index : mongoCollection.listIndexes()) {
      Object _id = index.get("_id");
      if(_id instanceof ObjectId) {
        _id = ((ObjectId) _id).toHexString();
      }
      if(_id != null) index.put("_id", _id);
      listResult.add(index);
    }
    
    if(debug) System.out.println(logprefix + "listIndexes(" + collection + ") -> " + listResult);
    return listResult;
  }
  
  @Override
  public
  String writeFile(String filename, byte[] content, Map<String,?> mapMetadata)
    throws Exception
  {
    if(debug) {
      if(content == null) {
        System.out.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + ")...");
      }
      else {
        System.out.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + ")...");
      }
    }
    
    GridFSBucket gridFSBucket = GridFSBuckets.create(db);
    
    BasicDBObject filter = new BasicDBObject(FILE_NAME, filename);
    
    GridFSFile gridFSFile = gridFSBucket.find(filter).first();
    if(gridFSFile != null) {
      ObjectId _id = gridFSFile.getObjectId();
      if(_id != null) return _id.toHexString();
      return filename;
    }
    
    GridFSUploadOptions options = new GridFSUploadOptions();
    if(mapMetadata != null && !mapMetadata.isEmpty()) {
      options.metadata(toDocument(mapMetadata));
    }
    
    GridFSUploadStream uploadStream = null;
    try {
      uploadStream = gridFSBucket.openUploadStream(filename, options);
      uploadStream.write(content);
    }
    finally {
      if(uploadStream != null) uploadStream.close();
    }
    String id = uploadStream.getObjectId().toHexString();
    if(debug) {
      if(content == null) {
        System.out.println(logprefix + "writeFile(" + filename + ",null," + mapMetadata + ") -> " + id);
      }
      else {
        System.out.println(logprefix + "writeFile(" + filename + ",byte[" + content.length + "]," + mapMetadata + ") -> " + id);
      }
    }
    return id;
  }
  
  @Override
  public
  List<Map<String,Object>> findFiles(String filename, Map<String,?> mapMetadata)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapMetadata + ")...");
    
    GridFSBucket gridFSBucket = GridFSBuckets.create(db);
    
    Map mapFilter = new HashMap();
    if(mapMetadata != null) {
      mapFilter.putAll(mapMetadata);
    }
    if(filename != null) {
      mapFilter.put(FILE_NAME, filename.replace('*', '%'));
    }
    BasicDBObject filter = buildQueryFilter(mapFilter);
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    MongoCursor<GridFSFile> mongoCursor = null;
    try {
      GridFSFindIterable gridFSFindIterable = gridFSBucket.find(filter);
      if(gridFSFindIterable == null) {
        if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapMetadata + ") -> 0 files (gridFSFindIterable = null)");
        return Collections.EMPTY_LIST;
      }
      mongoCursor = gridFSFindIterable.iterator();
      
      while(mongoCursor.hasNext()) {
        GridFSFile gridFSFile = mongoCursor.next();
        
        Map<String,Object> mapRecord = new HashMap<String,Object> ();
        Map mapFileMetadata = gridFSFile.getMetadata();
        if(mapFileMetadata != null) mapRecord.putAll(mapFileMetadata);
        mapRecord.put(FILE_NAME,        gridFSFile.getFilename());
        mapRecord.put(FILE_LENGTH,      gridFSFile.getLength());
        mapRecord.put(FILE_DATE_UPLOAD, gridFSFile.getUploadDate());
        mapRecord.put(FILE_MD5,         gridFSFile.getMD5());
        
        listResult.add(mapRecord);
      }
    }
    finally {
      if(mongoCursor != null) try{ mongoCursor.close(); } catch(Exception ex) {}
    }
    if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapMetadata + ") -> " + listResult.size() + " files");
    return listResult;
  }
  
  @Override
  public
  Map<String,Object> readFile(String filename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "readFile(" + filename + ")...");
    
    GridFSBucket gridFSBucket = GridFSBuckets.create(db);
    
    Map<String,Object> mapResult = null;
    GridFSDownloadStream downloadStream = null;
    try {
      downloadStream = gridFSBucket.openDownloadStream(filename);
      
      if(downloadStream == null) return null;
      
      GridFSFile gridFSFile = downloadStream.getGridFSFile();
      
      if(gridFSFile == null) return null;
      
      int length = (int) gridFSFile.getLength();
      
      byte[] content = new byte[length];
      
      int pos = 0;
      int rem = length;
      while(rem > 0) {
        int read = downloadStream.read(content, pos, rem);
        rem = read > 0 ? rem - read : 0;
        pos += read;
      }
      
      mapResult = new HashMap<String,Object>();
      
      Map mapFileMetadata = gridFSFile.getMetadata();
      if(mapFileMetadata != null) mapResult.putAll(mapFileMetadata);
      mapResult.put(FILE_NAME,        gridFSFile.getFilename());
      mapResult.put(FILE_CONTENT,     content);
      mapResult.put(FILE_LENGTH,      gridFSFile.getLength());
      mapResult.put(FILE_DATE_UPLOAD, gridFSFile.getUploadDate());
      mapResult.put(FILE_MD5,         gridFSFile.getMD5());
    }
    finally {
      if(downloadStream != null) try{ downloadStream.close(); } catch(Throwable th) {}
    }
    if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> {" + mapResult.size() + "}");
    return mapResult;
  }
  
  @Override
  public
  boolean removeFile(String filename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ")...");
    
    GridFSBucket gridFSBucket = GridFSBuckets.create(db);
    
    BasicDBObject filter = new BasicDBObject(FILE_NAME, filename);
    
    GridFSFile gridFSFile = gridFSBucket.find(filter).first();
    
    if(gridFSFile == null) {
      if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> false (gridFSFile = null)");
      return false;
    }
    
    ObjectId objectId = gridFSFile.getObjectId();
    if(objectId == null) {
      if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> false (objectId = null)");
      return false;
    }
    
    gridFSBucket.delete(objectId);
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> true");
    return true;
  }
  
  @Override
  public
  boolean renameFile(String filename, String newFilename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ")...");
    
    GridFSBucket gridFSBucket = GridFSBuckets.create(db);
    
    BasicDBObject filter = new BasicDBObject(FILE_NAME, filename);
    
    GridFSFile gridFSFile = gridFSBucket.find(filter).first();
    
    if(gridFSFile == null) {
      if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> false (gridFSFile = null)");
      return false;
    }
    
    ObjectId objectId = gridFSFile.getObjectId();
    if(objectId == null) {
      if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> false (objectId = null)");
      return false;
    }
    
    gridFSBucket.rename(objectId, newFilename);
    
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> true");
    return true;
  }
  
  public static
  String getId(Document document)
  {
    if(document == null) return null;
    Object _id = document.get("_id");
    if(_id instanceof ObjectId) {
      _id = ((ObjectId) _id).toHexString();
    }
    if(_id != null) return _id.toString();
    return null;
  }
  
  public static
  Document toDocument(Object object)
  {
    if(object == null) return null;
    if(object instanceof Map) {
      Map map = (Map) object;
      normalizeMap(map);
      Object _id = map.get("_id");
      Document result = new Document(map);
      if(_id != null) {
        String sId = _id.toString();
        if(sId.length() == 24) {
          result.put("_id", new ObjectId(sId));
        }
      }
      return result;
    }
    return null;
  }
  
  public static
  Map toMap(DBObject dbObject)
  {
    if(dbObject == null) return null;
    Object _id = dbObject.get("_id");
    if(_id instanceof ObjectId) {
      _id = ((ObjectId) _id).toHexString();
    }
    Map mapResult = dbObject.toMap();
    if(_id != null) {
      mapResult.put("_id", _id);
    }
    return mapResult;
  }
  
  public static
  BasicDBObject buildQueryFilter(String id)
  {
    BasicDBObject result = null;
    if(id != null && id.length() == 24) {
      result = new BasicDBObject("_id", new ObjectId(id));
    }
    else {
      result = new BasicDBObject("id", id);
    }
    return result;
  }
  
  public static
  BasicDBObject buildQueryFilter(Map map)
  {
    BasicDBObject result = new BasicDBObject();
    
    if(map == null || map.isEmpty()) return result;
    normalizeMap(map);
    
    Iterator iterator = map.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey  = entry.getKey();
      String sKey  = oKey.toString();
      if(sKey.equals(FILTER_EXCLUDE) || sKey.equals(FILTER_FIELDS)) continue;
      
      Object value = entry.getValue();
      if(value instanceof Collection) {
        result.put(sKey, new BasicDBObject("$in", value));
        continue;
      }
      if(value != null && value.getClass().isArray()) {
        result.put(sKey, new BasicDBObject("$in", value));
        continue;
      }
      if(sKey.equals("_id")) {
        if(value instanceof String) {
          String sValue = value.toString();
          if(sValue.length() == 24) {
            result.put(sKey, new ObjectId(sValue));
            continue;
          }
          if(sValue.equals("true")) {
            result.put(sKey, Boolean.TRUE);
            continue;
          }
          if(sValue.equals("false")) {
            result.put(sKey, Boolean.FALSE);
            continue;
          }
        }
      }
      
      boolean boStartsWithPerc = false;
      boolean boEndsWithPerc   = false;
      boStartsWithPerc = sKey.startsWith("%");
      if(boStartsWithPerc) sKey = sKey.substring(1);
      boEndsWithPerc = sKey.endsWith("%");
      if(boEndsWithPerc) sKey = sKey.substring(0, sKey.length()-1);
      
      boolean boGTE  = sKey.startsWith(">=");
      boolean boLTE  = sKey.startsWith("<=");
      boolean boNE   = sKey.startsWith("<>");
      if(!boNE) boNE = sKey.startsWith("!=");
      if(boGTE || boLTE || boNE) {
        sKey = sKey.substring(2);
      }
      else {
        boGTE  = sKey.endsWith(">=");
        boLTE  = sKey.endsWith("<=");
        boNE   = sKey.endsWith("<>");
        if(!boNE) boNE = sKey.endsWith("!=");
        if(boGTE || boLTE || boNE) {
          sKey = sKey.substring(0, sKey.length()-2);
        }
      }
      
      boolean boGT  = sKey.startsWith(">");
      boolean boLT  = sKey.startsWith("<");
      if(boGT || boLT) {
        sKey = sKey.substring(1);
      }
      else {
        boGT = sKey.endsWith(">");
        boLT = sKey.endsWith("<");
        if(boGT || boLT) {
          sKey = sKey.substring(0, sKey.length()-1);
        }
      }
      
      if(value instanceof Calendar) {
        value = ((Calendar) value).getTime();
      }
      
      if(value == null || "null".equals(value)) {
        if(boNE) {
          result.put(sKey, new BasicDBObject("$exists", true));
        }
        else {
          result.put(sKey, new BasicDBObject("$exists", false));
        }
        continue;
      }
      
      if(value instanceof String) {
        String sValue = ((String) value).trim();
        if(sValue.length() == 0) continue;
        
        if(sValue.startsWith("%")) {
          sValue = sValue.substring(1);
          boStartsWithPerc = true;
        }
        if(sValue.endsWith("%")) {
          sValue = sValue.substring(0, sValue.length()-1);
          boEndsWithPerc = true;
        }
        if(boStartsWithPerc || boEndsWithPerc) {
          String sRegExp = "^";
          if(boStartsWithPerc) sRegExp += ".*";
          sRegExp += sValue.replace("%", ".*");
          if(boEndsWithPerc) sRegExp += ".*";
          sRegExp += "$";
          result.put(sKey, java.util.regex.Pattern.compile(sRegExp));
          continue;
        }
        
        String sVal = (String) value;
        if(!(boGTE || boLTE || boNE || boGT || boLT)) {
          boGTE  = sVal.startsWith(">=");
          boLTE  = sVal.startsWith("<=");
          boNE   = sVal.startsWith("<>");
          if(!boNE) boNE = sVal.startsWith("!=");
          if(boGTE || boLTE || boNE) sVal = sVal.substring(2);
          
          boGT   = sVal.startsWith(">");
          boLT   = sVal.startsWith("<");
          if(boGT || boLT) sVal = sVal.substring(1);
          
          if(boGTE || boLTE || boNE || boGT || boLT) {
            value = JSON.parse(sVal);
          }
        }
      }
      
      if(boNE) {
        Object prev = result.get(sKey);
        if(prev instanceof BasicDBObject) {
          ((BasicDBObject) prev).put("$ne", value);
        }
        else {
          result.put(sKey, new BasicDBObject("$ne", value));
        }
      }
      else
      if(boGT) {
        Object prev = result.get(sKey);
        if(prev instanceof BasicDBObject) {
          ((BasicDBObject) prev).put("$gt", value);
        }
        else {
          result.put(sKey, new BasicDBObject("$gt", value));
        }
      }
      else
      if(boLT) {
        Object prev = result.get(sKey);
        if(prev instanceof BasicDBObject) {
          ((BasicDBObject) prev).put("$lt", value);
        }
        else {
          result.put(sKey, new BasicDBObject("$lt", value));
        }
      }
      else
      if(boGTE) {
        Object prev = result.get(sKey);
        if(prev instanceof BasicDBObject) {
          ((BasicDBObject) prev).put("$gte", value);
        }
        else {
          result.put(sKey, new BasicDBObject("$gte", value));
        }
      }
      else
      if(boLTE) {
        Object prev = result.get(sKey);
        if(prev instanceof BasicDBObject) {
          ((BasicDBObject) prev).put("$lte", value);
        }
        else {
          result.put(sKey, new BasicDBObject("$lte", value));
        }
      }
      else {
        result.put(sKey, value);
      }
    }
    return result;
  }
  
  protected static
  String getOrderField(String sOrderClause)
  {
    int iSep = sOrderClause.indexOf(' ');
    if(iSep > 0) return sOrderClause.substring(0, iSep);
    return sOrderClause;
  }
  
  protected static
  int getOrderType(String sOrderClause)
  {
    int iSep = sOrderClause.indexOf(' ');
    if(iSep > 0) {
      char c0 = sOrderClause.substring(iSep+1).charAt(0);
      if(c0 == 'd' || c0 == 'D' || c0 == '-') return -1;
      return 1;
    }
    return 1;
  }
  
  protected static
  void normalizeMap(Map<String,Object> mapData)
  {
    if(mapData == null || mapData.isEmpty()) return;
    Iterator<Map.Entry<String, Object>> iterator = mapData.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      Object val = entry.getValue();
      if(val instanceof String) {
        if(val.equals("true"))  entry.setValue(Boolean.TRUE);  else
        if(val.equals("false")) entry.setValue(Boolean.FALSE); else {
          entry.setValue(val);
        }
        continue;
      }
      entry.setValue(val);
    }
  }
  
  public static
  MongoClient getMongoClient()
    throws Exception
  {
    try {
      MongoClient mongoClient = mapMDBClient.get("java:global/mongoClient");
      if(mongoClient != null) return mongoClient;
      
      Context ctx = new InitialContext();
      Object lookup = ctx.lookup("java:global/mongoClient");
      if(lookup instanceof MongoClient){
        mongoClient = MongoClient.class.cast(lookup);
        mapMDBClient.put("java:global/mongoClient", mongoClient);
        return mongoClient;
      }
      
      if(lookup instanceof String){
        mongoClient = mapMDBClient.get(lookup);
        if(mongoClient != null) return mongoClient;
        
        String sURI = (String) lookup;
        mongoClient = new MongoClient(new MongoClientURI(sURI));
        mapMDBClient.put(sURI, mongoClient);
        return mongoClient;
      }
    } 
    catch (Exception ex) {
      System.out.println("Error in lookup java:global/mongoClient: " + ex);
    }
    
    String sUrl = BEConfig.getProperty("nosqldb.uri");
    if(sUrl == null) sUrl = BEConfig.getProperty("nosqldb.url");
    if(sUrl != null && sUrl.length() > 0 && sUrl.startsWith("mongodb://")) {
      MongoClient mongoClient = mapMDBClient.get(sUrl);
      if(mongoClient != null) return mongoClient;
      
      mongoClient = new MongoClient(new MongoClientURI(sUrl));
      mapMDBClient.put(sUrl, mongoClient);
      return mongoClient;
    }
    
    String sHost   = BEConfig.getProperty("nosqldb.host");
    String sPort   = BEConfig.getProperty("nosqldb.port");
    String sUser   = BEConfig.getProperty("nosqldb.user");
    String sPass   = BEConfig.getProperty("nosqldb.pass");
    String sDbAuth = BEConfig.getProperty("nosqldb.dbauth");
    return getMongoClient(sHost, sPort, sUser, sPass, sDbAuth);
  }
  
  public static
  MongoClient getMongoClient(String sHost, String sPort, String sUser, String sPass, String sDbAuth)
    throws Exception
  {
    if(sHost == null || sHost.length() == 0) sHost = "127.0.0.1";
    int iPort = 0;
    if(sPort != null && sPort.length() > 0) {
      try{ iPort = Integer.parseInt(sPort); } catch(Exception ex) {}
    }
    if(iPort < 100) iPort = 27017;
    MongoClient mongoClient = mapMDBClient.get(sUser + "#" + sDbAuth + "@" + sHost + ":" + iPort);
    if(mongoClient == null) {
      if(sUser != null && sUser.length() > 0) {
        if(sDbAuth == null || sDbAuth.length() == 0) sDbAuth = "admin";
        char[] password = sPass != null ? sPass.toCharArray() : new char[0];
        MongoCredential mongoCredential = MongoCredential.createCredential(sUser, sDbAuth, password);
        mongoClient = new MongoClient(new ServerAddress(sHost, iPort), Arrays.asList(mongoCredential));
      }
      else {
        mongoClient = new MongoClient(new ServerAddress(sHost, iPort));
      }
      mapMDBClient.put(sUser + "#" + sDbAuth + "@" + sHost + ":" + iPort, mongoClient);
    }
    return mongoClient;
  }
}
