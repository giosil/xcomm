package org.dew.xcomm.nosql;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
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

import org.bson.types.ObjectId;

import org.dew.xcomm.nosql.json.JSON;
import org.dew.xcomm.nosql.util.WUtil;
import org.dew.xcomm.util.BEConfig;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

@SuppressWarnings({"rawtypes", "unchecked"})
public
class NoSQLMongoDB2 implements INoSQLDB
{
  protected static String logprefix = NoSQLMongoDB2.class.getSimpleName() + ".";
  protected static Map<String, MongoClient> mapMDBClient = new HashMap<String, MongoClient> ();
  protected static Set<String> indexesCreated;
  
  protected DB db;
  protected boolean debug = false;
  protected int defLimit  = 10000;
  
  @SuppressWarnings("deprecation")
  public NoSQLMongoDB2()
    throws Exception
  {
    String dbname = BEConfig.getDefaultDbName();
    this.db = getMongoClient().getDB(dbname);
  }
  
  @SuppressWarnings("deprecation")
  public NoSQLMongoDB2(String dbname)
    throws Exception
  {
    if(dbname == null || dbname.length() == 0) dbname = BEConfig.getDefaultDbName();
    this.db = getMongoClient().getDB(dbname);
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
    
    CommandResult commandResult = db.command(new BasicDBObject("buildinfo", 1));
    if(commandResult != null) {
      mapResult.put("name",    "mongodb");
      mapResult.put("version", commandResult.get("version"));
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
    
    Set<String> collectionNames = db.getCollectionNames();
    
    int size = collectionNames != null ? collectionNames.size() : 0;
    
    List<String> result = new ArrayList<String> (size);
    
    if(size > 0) {
      for(String collectionName : collectionNames) {
        result.add(collectionName);
      }
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    dbCollection.drop();
    
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    DBObject dbObject = toDBObject(mapData);
    
    if(debug) System.out.println(logprefix + "insert " + collection + ".insert(" + dbObject + ")...");
    
    dbCollection.insert(dbObject);
    
    String result = getId(dbObject);
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String,?> mapData, boolean refresh)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    DBObject dbObject = toDBObject(mapData);
    
    if(debug) System.out.println(logprefix + "insert " + collection + ".insert(" + dbObject + ")...");
    
    dbCollection.insert(dbObject);
    
    String result = getId(dbObject);
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    if(debug) System.out.println(logprefix + "bulkIns " + collection + ".initializeOrderedBulkOperation()...");
    BulkWriteOperation bulkWriteOperation = dbCollection.initializeOrderedBulkOperation();
    for(int i = 0; i < listData.size(); i++) {
      DBObject dbObject = toDBObject(listData.get(i));
      if(debug) System.out.println(logprefix + "bulkIns bulkWriteOperation.insert(" + dbObject + ")...");
      bulkWriteOperation.insert(toDBObject(listData.get(i)));
    }
    
    if(debug) System.out.println(logprefix + "bulkIns bulkWriteOperation.execute()...");
    BulkWriteResult bulkWriteResult = bulkWriteOperation.execute();
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    DBObject dbObject = toDBObject(mapData);
    
    BasicDBObject query = buildQueryFilter(id);
    
    if(debug) System.out.println(logprefix + "replace " + collection + ".update(" + query + "," + dbObject +")...");
    
    WriteResult wr = dbCollection.update(query, dbObject);
    
    boolean result = wr.getN() > 0;
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    DBObject dbObject = toDBObject(mapData);
    
    BasicDBObject query = buildQueryFilter(id);
    
    BasicDBObject dbset = new BasicDBObject("$set", dbObject);
    
    if(debug) System.out.println(logprefix + "update " + collection + ".update(" + query + "," + dbset +")...");
    
    WriteResult wr = dbCollection.update(query, dbset);
    
    int result = wr.getN();
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    DBObject dbObject = toDBObject(mapData);
    
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    BasicDBObject dbset = new BasicDBObject("$set", dbObject);
    
    if(debug) System.out.println(logprefix + "update " + collection + ".update(" + query + "," + dbset + ",false,true)...");
    
    WriteResult wr = dbCollection.update(query, dbset, false, true);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String upsert(String collection, Map<String,?> mapData, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    DBObject dbObject = toDBObject(mapData);
    
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    BasicDBObject dbset = new BasicDBObject("$set", dbObject);
    
    if(debug) System.out.println(logprefix + "update " + collection + ".update(" + query + "," + dbset + ", true, false)...");
    
    WriteResult wr = dbCollection.update(query, dbset, true, false);
    
    Object upsertedId = wr.getUpsertedId();
    String id = null;
    if(upsertedId instanceof ObjectId) {
      id = ((ObjectId) upsertedId).toHexString();
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
    
    BasicDBObject dbObject = new BasicDBObject();
    int iIndexOf  = 0;
    int iBegin    = 0;
    iIndexOf      = fields.indexOf(',');
    while(iIndexOf >= 0) {
      dbObject.append(fields.substring(iBegin,iIndexOf),"");
      iIndexOf = fields.indexOf(',', iBegin=iIndexOf+1);
    }
    dbObject.append(fields.substring(iBegin),"");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(id);
    
    BasicDBObject dbunset = new BasicDBObject("$unset", dbObject);
    
    if(debug) System.out.println(logprefix + "unset " + collection + ".update(" + query + "," + dbunset +")...");
    
    WriteResult wr = dbCollection.update(query, dbunset);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, String id, String field, Number value)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(id);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field, value));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".update(" + query + "," + dbinc + ")...");
    
    WriteResult wr = dbCollection.update(query, dbinc);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, String id, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(id);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field1, value1).append(field2, value2));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".update(" + query + "," + dbinc + ")...");
    
    WriteResult wr = dbCollection.update(query, dbinc);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, Map<String,?> mapFilter, String field, Number value)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field, value));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".update(" + query + "," + dbinc + ",false,true)...");
    
    WriteResult wr = dbCollection.update(query, dbinc, false, true);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int inc(String collection, Map<String,?> mapFilter, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    BasicDBObject dbinc = new BasicDBObject("$inc", new BasicDBObject().append(field1, value1).append(field2, value2));
    
    if(debug) System.out.println(logprefix + "inc " + collection + ".update(" + query + "," + dbinc + ",false,true)...");
    
    WriteResult wr = dbCollection.update(query, dbinc, false, true);
    
    int result = wr.getN();
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    if(id.equals("!")) {
      if(debug) System.out.println(logprefix + "delete " + collection + ".drop()");
      dbCollection.drop();
      return 1;
    }
    
    BasicDBObject query = buildQueryFilter(id);
    
    if(debug) System.out.println(logprefix + "delete " + collection + ".remove(" + query + ")");
    
    WriteResult wr = dbCollection.remove(query);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> " + result);
    return result;
  }
  
  @Override
  public
  int delete(String collection, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    if(debug) System.out.println(logprefix + "delete " + collection + ".remove(" + query + ")");
    
    WriteResult wr = dbCollection.remove(query);
    
    int result = wr.getN();
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\")...");
    
    DBObject keys   = null;
    String[] toexcl = null;
    String[] askeys = null;
    if(mapFilter != null) {
      toexcl = WUtil.toArrayOfString(mapFilter.get(FILTER_EXCLUDE), false);
      if(fields == null || fields.length() == 0 || fields.equals("*")) {
        askeys = WUtil.toArrayOfString(mapFilter.get(FILTER_FIELDS), false);
      }
      else {
        askeys = WUtil.toArrayOfString(fields, false);
      }
      if(askeys != null && askeys.length > 0) {
        keys = new BasicDBObject();
        for(int i = 0; i < askeys.length; i++) {
          keys.put(askeys[i], 1);
        }
      }
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    DBCursor dbCursor = null;
    try {
      DBCollection dbCollection = db.getCollection(collection);
      
      BasicDBObject query = buildQueryFilter(mapFilter);
      
      if(debug) System.out.println(logprefix + "find " + collection + ".find(" + query + "," + keys + ").limit(" + defLimit + ")");
      
      dbCursor = dbCollection.find(query, keys).limit(defLimit);
      while(dbCursor.hasNext()) {
        DBObject dbObject = dbCursor.next();
        
        if(toexcl != null) {
          for(int i = 0; i < toexcl.length; i++) {
            dbObject.removeField(toexcl[i]);
          }
        }
        
        Object _id = dbObject.get("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        Map mapRecord = dbObject.toMap();
        if(_id != null) mapRecord.put("_id", _id);
        
        listResult.add(mapRecord);
      }
    }
    finally {
      if(dbCursor != null) try{ dbCursor.close(); } catch(Exception ex) {}
    }
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields, String orderBy, int limit)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ")...");
    
    DBObject keys   = null;
    String[] toexcl = null;
    String[] askeys = null;
    if(mapFilter != null) {
      toexcl = WUtil.toArrayOfString(mapFilter.get(FILTER_EXCLUDE), false);
      if(fields == null || fields.length() == 0 || fields.equals("*")) {
        askeys = WUtil.toArrayOfString(mapFilter.get(FILTER_FIELDS), false);
      }
      else {
        askeys = WUtil.toArrayOfString(fields, false);
      }
      if(askeys != null && askeys.length > 0) {
        keys = new BasicDBObject();
        for(int i = 0; i < askeys.length; i++) {
          keys.put(askeys[i], 1);
        }
      }
    }
    if(limit < 1) limit = defLimit;
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    DBCursor dbCursor = null;
    try {
      DBCollection dbCollection = db.getCollection(collection);
      
      BasicDBObject query = buildQueryFilter(mapFilter);
      
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
        if(debug) System.out.println(logprefix + "find " + collection + ".find(" + query + ").sort(" + dbsort + ").limit(" + limit + ")");
        dbCursor = dbCollection.find(query, keys).sort(dbsort).limit(limit);
      }
      else {
        if(debug) System.out.println(logprefix + "find " + collection + ".find(" + query + ").limit(" + limit + ")");
        dbCursor = dbCollection.find(query, keys).limit(limit);
      }
      while(dbCursor.hasNext()) {
        DBObject dbObject = dbCursor.next();
        
        if(toexcl != null) {
          for(int i = 0; i < toexcl.length; i++) {
            dbObject.removeField(toexcl[i]);
          }
        }
        
        Object _id = dbObject.get("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        Map mapRecord = dbObject.toMap();
        if(_id != null) mapRecord.put("_id", _id);
        
        listResult.add(mapRecord);
      }
    }
    finally {
      if(dbCursor != null) try{ dbCursor.close(); } catch(Exception ex) {}
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
    DBCursor dbCursor = null;
    try {
      DBCollection dbCollection = db.getCollection(collection);
      
      if(indexesCreated == null) indexesCreated = new HashSet<String> ();
      if(!indexesCreated.contains(collection + "." + field)) {
        if(debug) System.out.println(logprefix + "search " + collection + ".createIndex({\"" + field + "\":\"text\"})...");
        dbCollection.createIndex(new BasicDBObject(field, "text"));
        indexesCreated.add(collection + "." + field);
      }
      
      BasicDBObject query = new BasicDBObject("$text", new BasicDBObject("$search", text));
      
      if(debug) System.out.println(logprefix + "find " + collection + ".find(" + query + ").limit(" + defLimit + ")");
      
      dbCursor = dbCollection.find(query).limit(defLimit);
      while(dbCursor.hasNext()) {
        DBObject dbObject = dbCursor.next();
        
        Object _id = dbObject.get("_id");
        if(_id instanceof ObjectId) {
          _id = ((ObjectId) _id).toHexString();
        }
        Map mapRecord = dbObject.toMap();
        if(_id != null) mapRecord.put("_id", _id);
        
        listResult.add(mapRecord);
      }
    }
    finally {
      if(dbCursor != null) try{ dbCursor.close(); } catch(Exception ex) {}
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
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
    
    BasicDBObject group = new BasicDBObject("_id", "$" + field);
    group.put(alias, new BasicDBObject(funct, argum));
    
    List<DBObject> pipeline = new ArrayList<DBObject>(2);
    pipeline.add(new BasicDBObject("$match", buildQueryFilter(mapFilter)));
    pipeline.add(new BasicDBObject("$group", group));
    
    Iterator<DBObject> iterator = dbCollection.aggregate(pipeline).results().iterator();
    while(iterator.hasNext()) {
      DBObject dbObject = iterator.next();
      
      Object _id = dbObject.get("_id");
      if(_id instanceof ObjectId) {
        _id = ((ObjectId) _id).toHexString();
      }
      Map mapRecord = dbObject.toMap();
      if(_id != null) mapRecord.put(field, _id);
      Object value = dbObject.get(alias);
      if(value != null) mapRecord.put("value", value);
      
      listResult.add(mapRecord);
    }
    if(debug) System.out.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  @Override
  public
  Map<String,Object> read(String collection, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(id);
    
    if(debug) System.out.println(logprefix + "read " + collection + ".findOne(" + query + ")");
    
    DBObject dbObject = dbCollection.findOne(query);
    
    if(dbObject == null) {
      if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> null");
      return null;
    }
    
    Object _id = dbObject.get("_id");
    if(_id instanceof ObjectId) {
      _id = ((ObjectId) _id).toHexString();
    }
    Map mapResult = dbObject.toMap();
    if(_id != null) mapResult.put("_id", _id);
    
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> " + mapResult);
    return mapResult;
  }
  
  @Override
  public
  int count(String collection, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ")...");
    
    DBCollection dbCollection = db.getCollection(collection);
    
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    if(debug) System.out.println(logprefix + "count " + collection + ".count(" + query + ")");
    
    int result = (int) dbCollection.count(query);
    
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
    
    if(type == 0) type = 1;
    
    DBCollection dbCollection = db.getCollection(collection);
    
    if(indexesCreated == null) indexesCreated = new HashSet<String> ();
    if(!indexesCreated.contains(collection + "." + field)) {
      if(debug) System.out.println(logprefix + "createIndex " + collection + ".createIndex({\"" + field + "\":" + type + "})...");
      dbCollection.createIndex(new BasicDBObject(field, type));
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
    
    DBCollection dbCollection = db.getCollection(collection);
    
    for(DBObject index : dbCollection.getIndexInfo()) {
      Object _id = index.get("_id");
      if(_id instanceof ObjectId) {
        _id = ((ObjectId) _id).toHexString();
      }
      if(_id != null) index.put("_id", _id);
      listResult.add(index.toMap());
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
    
    GridFS gridFS = new GridFS(db);
    
    GridFSDBFile gridFSDBFile = gridFS.findOne(filename);
    if(gridFSDBFile != null) return getId(gridFSDBFile);
    
    GridFSInputFile gridFSInputFile = gridFS.createFile(content);
    gridFSInputFile.setFilename(filename);
    gridFSInputFile.setMetaData(toDBObject(mapMetadata));
    gridFSInputFile.save();
    String id = getId(gridFSInputFile);
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
    
    GridFS gridFS = new GridFS(db);
    
    Map mapFilter = new HashMap();
    if(mapMetadata != null) {
      mapFilter.putAll(mapMetadata);
    }
    if(filename != null) {
      mapFilter.put(FILE_NAME, filename.replace('*', '%'));
    }
    BasicDBObject query = buildQueryFilter(mapFilter);
    
    List<GridFSDBFile> listOfGridFSDBFile = gridFS.find(query);
    if(listOfGridFSDBFile == null) {
      if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapMetadata + ") -> 0 files (listOfGridFSDBFile = null)");
      return Collections.EMPTY_LIST;
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> (listOfGridFSDBFile.size());
    for(int i = 0; i < listOfGridFSDBFile.size(); i++) {
      GridFSDBFile gridFSDBFile = listOfGridFSDBFile.get(i);
      
      Map<String,Object> mapRecord = new HashMap<String,Object> ();
      Map mapFileMetadata = toMap(gridFSDBFile.getMetaData());
      if(mapFileMetadata != null) mapRecord.putAll(mapFileMetadata);
      mapRecord.put(FILE_NAME,        gridFSDBFile.getFilename());
      mapRecord.put(FILE_LENGTH,      gridFSDBFile.getLength());
      mapRecord.put(FILE_DATE_UPLOAD, gridFSDBFile.getUploadDate());
      mapRecord.put(FILE_MD5,         gridFSDBFile.getMD5());
      
      listResult.add(mapRecord);
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
    
    GridFS gridFS = new GridFS(db);
    
    byte[] content = null;
    GridFSDBFile gridFSDBFile = gridFS.findOne(filename);
    if(gridFSDBFile != null) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      gridFSDBFile.writeTo(baos);
      content = baos.toByteArray();
    }
    
    Map<String,Object> mapResult = new HashMap<String,Object> ();
    Map mapFileMetadata = toMap(gridFSDBFile.getMetaData());
    if(mapFileMetadata != null) mapResult.putAll(mapFileMetadata);
    mapResult.put(FILE_NAME,        gridFSDBFile.getFilename());
    mapResult.put(FILE_CONTENT,     content);
    mapResult.put(FILE_LENGTH,      gridFSDBFile.getLength());
    mapResult.put(FILE_DATE_UPLOAD, gridFSDBFile.getUploadDate());
    mapResult.put(FILE_MD5,         gridFSDBFile.getMD5());
    
    if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> {" + mapResult.size() + "}");
    return mapResult;
  }
  
  @Override
  public
  boolean removeFile(String filename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ")...");
    
    GridFS gridFS = new GridFS(db);
    
    GridFSDBFile gridFSDBFile = gridFS.findOne(filename);
    if(gridFSDBFile == null) {
      if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> false (gridFSDBFile = null)");
      return false;
    }
    
    gridFS.remove(filename);
    
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> true");
    return true;
  }
  
  @Override
  public
  boolean renameFile(String filename, String newFilename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ")...");
    
    GridFS gridFS = new GridFS(db);
    
    GridFSDBFile gridFSDBFile = gridFS.findOne(filename);
    if(gridFSDBFile == null) {
      if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> false (gridFSDBFile = null)");
      return false;
    }
    
    gridFSDBFile.put("filename", newFilename);
    gridFSDBFile.save();
    
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> true");
    return true;
  }
  
  public static
  String getId(DBObject dbObject)
  {
    if(dbObject == null) return null;
    Object _id = dbObject.get("_id");
    if(_id instanceof ObjectId) {
      _id = ((ObjectId) _id).toHexString();
    }
    if(_id != null) return _id.toString();
    return null;
  }
  
  public static
  DBObject toDBObject(Object object)
  {
    if(object == null) return null;
    if(object instanceof Collection) {
      BasicDBList result = new BasicDBList();
      Iterator iterator = ((Collection) object).iterator();
      while(iterator.hasNext()) {
        result.add(toDBObject(iterator.next()));
      }
      return result;
    }
    else
    if(object instanceof Map) {
      Map map = (Map) object;
      normalizeMap(map);
      Object _id = map.get("_id");
      BasicDBObject result = new BasicDBObject(map);
      if(_id != null) {
        String sId = _id.toString();
        if(sId.length() == 24) {
          result.put("_id", new ObjectId(sId));
        }
      }
      return result;
    }
    else
    if(object.getClass().isArray()) {
      int length = Array.getLength(object);
      BasicDBList result = new BasicDBList();
      for(int i = 0; i < length; i++) {
        result.add(toDBObject(Array.get(object, i)));
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
