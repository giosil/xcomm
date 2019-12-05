package org.dew.xcomm.nosql;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import java.lang.reflect.Array;

import java.net.HttpURLConnection;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

import java.nio.ByteBuffer;

import java.security.MessageDigest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.dew.xcomm.nosql.json.JSON;

import org.dew.xcomm.nosql.util.Base64Coder;

import org.dew.xcomm.nosql.util.WMap;
import org.dew.xcomm.nosql.util.WUtil;
import org.dew.xcomm.util.BEConfig;

/**
 * Implementazione di INoSQLDB per Elasticsearch.
 * Per l'implementazione di inc in config/elasticsearch.yml aggiungere:
 *
 * script.inline: true
 * script.stored: true
 *
 */
@SuppressWarnings({"rawtypes","unchecked"})
public
class NoSQLElasticsearch implements INoSQLDB
{
  protected static String logprefix = NoSQLElasticsearch.class.getSimpleName() + ".";
  
  protected boolean debug    = false;
  protected String  host     = BEConfig.getProperty("nosqldb.host",    "localhost");
  protected int     port     = BEConfig.getIntProperty("nosqldb.port", 9200);
  protected String  user     = BEConfig.getProperty("nosqldb.user");
  protected String  pass     = BEConfig.getProperty("nosqldb.pass",    "");
  protected String  index    = BEConfig.getProperty("nosqldb.dbname",  BEConfig.getProperty("nosqldb.dbauth"));
  protected int     defLimit = 10000;
  protected int     timeOut  = 60000;
  
  public NoSQLElasticsearch()
  {
  }
  
  public NoSQLElasticsearch(String index)
  {
    if(index != null && index.length() > 0) {
      this.index = index;
    }
  }
  
  public NoSQLElasticsearch(String host, int port, String index)
  {
    if(host != null && host.length() > 0) {
      this.host = host;
    }
    if(port  > 0) {
      this.port = port;
    }
    if(index != null && index.length() > 0) {
      this.index = index;
    }
  }
  
  public NoSQLElasticsearch(String host, int port, String index, String user, String pass)
  {
    if(host != null && host.length() > 0) {
      this.host = host;
    }
    if(port  > 0) {
      this.port = port;
    }
    if(index != null && index.length() > 0) {
      this.index = index;
    }
    this.user = user;
    this.pass = pass;
    if(this.pass == null) this.pass="";
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
    
    String url = getURL("..", null);
    
    WMap resGET = http("GET", url, null);
    
    Map<String,Object> mapResult = new HashMap<String,Object> (2);
    mapResult.put("name", resGET.getString("cluster_name"));
    
    Map mapVersion = resGET.getMap("version");
    if(mapVersion != null) {
      mapResult.put("version", mapVersion.get("number"));
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
    
    String url = getURL(null, null);
    
    WMap resGET = null;
    try {
      resGET = http("GET", url, null);
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      return new ArrayList<String> (0);
    }
    
    Map mapIndex = resGET.getMap(index);
    
    List<String> listResult = new ArrayList<String> ();
    
    if(mapIndex != null) {
      Object mappings = mapIndex.get("mappings");
      if(mappings instanceof Map) {
        Map mapMappings = (Map) mappings;
        Iterator iterator = mapMappings.keySet().iterator();
        while(iterator.hasNext()) {
          Object keyMapping = iterator.next();
          listResult.add(keyMapping.toString());
        }
      }
    }
    
    Collections.sort(listResult);
    if(debug) System.out.println(logprefix + "getCollections() -> " + listResult);
    return listResult;
  }
  
  @Override
  public 
  boolean drop(String collection) 
    throws Exception
  {
    if(debug) System.out.println(logprefix + "drop(" + collection + ")...");
    boolean result = false;
    if(debug) System.out.println(logprefix + "drop(" + collection + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String,?> mapData)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ")...");
    
    String id   = generateId();
    String url  = getURL(collection, id);
    String data = mapData != null ? JSON.stringify(mapData) : "{}";
    
    WMap resPUT = http("PUT", url, data);
    
    String result = resPUT.getBoolean("created") ? id : null;
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + ") -> " + result);
    return result;
  }
  
  @Override
  public
  String insert(String collection, Map<String,?> mapData, boolean refresh)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "insert(" + collection + "," + mapData + "," + refresh + ")...");
    
    String id   = generateId();
    String url  = getURL(collection, id);
    String data = mapData != null ? JSON.stringify(mapData) : "{}";
    
    if(refresh) url += "?refresh=true";
    
    WMap resPUT = http("PUT", url, data);
    
    String result = resPUT.getBoolean("created") ? id : null;
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
    
    String url = getURL(collection, null, "_bulk");
    
    int countIns = 0;
    StringBuilder dbData = new StringBuilder();
    for(int i = 0; i < listData.size(); i++) {
      Map<String,?> mapData = listData.get(i);
      if(mapData == null || mapData.isEmpty()) continue;
      String id = generateId();
      dbData.append("{\"index\":{\"_id\":\"" + id + "\"}}\n");
      dbData.append(JSON.stringify(mapData));
      dbData.append("\n");
      countIns++;
    }
    
    WMap resPUT = http("PUT", url, dbData.toString());
    
    boolean errors = resPUT.getBoolean("errors");
    if(errors) {
      System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> 0 (errors = true)");
      return countIns;
    }
    System.out.println(logprefix + "bulkIns(" + collection + ", " + listData.size() + " documents) -> " + countIns);
    return countIns;
  }
  
  @Override
  public
  boolean replace(String collection, Map<String,?> mapData, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "replace(" + collection + "," + mapData + ")...");
    
    String url  = getURL(collection, id);
    String data = mapData != null ? JSON.stringify(mapData) : "{}";
    
    WMap resPUT = http("PUT", url, data);
    
    boolean result = resPUT.getBoolean("created");
    if(debug) System.out.println(logprefix + "replace(" + collection + "," + mapData + ") -> " + result);
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
    
    String url  = getURL(collection, id, "_update");
    
    String data = mapData != null ? JSON.stringify(mapData) : "{}";
    
    try {
      http("POST", url, "{\"doc\":" + data + "}");
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> 0 " + message);
      return 0;
    }
    
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + id + ") -> " + 1);
    return 1;
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
    
    String url  = getSearchURL(collection, mapFilter, null, 0, "_source=false");
    
    WMap resGET = http("GET", url, null);
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> 0 (hits=null)");
      return 0;
    }
    
    int countUpd = 0;
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      
      Object _id = mapHit.get("_id");
      if(_id == null) continue;
      
      String urlU = getURL(collection, _id, "_update");
      
      String data = mapData != null ? JSON.stringify(mapData) : "{}";
      try {
        http("POST", urlU, "{\"doc\":" + data + "}");
        countUpd++;
      }
      catch(Exception ex) {
        String message = ex.getMessage();
        if(message == null || !message.startsWith("{")) throw ex;
      }
    }
    if(debug) System.out.println(logprefix + "update(" + collection + "," + mapData + "," + mapFilter + ") -> " + countUpd);
    return countUpd;
  }
  
  @Override
  public
  String upsert(String collection, Map<String,?> mapData, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ")...");
    
    String url  = getSearchURL(collection, mapFilter, null, 0, "_source=false");
    
    WMap resGET = http("GET", url, null);
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "upsert(" + collection + "," + mapData + "," + mapFilter + ") -> null (hits=null)");
      return null;
    }
    
    int countUpd = 0;
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      
      Object _id = mapHit.get("_id");
      if(_id == null) continue;
      
      String urlU = getURL(collection, _id, "_update");
      
      String data = mapData != null ? JSON.stringify(mapData) : "{}";
      try {
        http("POST", urlU, "{\"doc\":" + data + "}");
        countUpd++;
      }
      catch(Exception ex) {
        String message = ex.getMessage();
        if(message == null || !message.startsWith("{")) throw ex;
      }
    }
    
    String id = null;
    if(countUpd == 0 && mapData != null && !mapData.isEmpty()) {
      id = generateId();
      String urlI = getURL(collection, id);
      String data = mapData != null ? JSON.stringify(mapData) : "{}";
      
      WMap resPUT = http("PUT", urlI, data);
      
      if(resPUT.getBoolean("created")) countUpd = 1;
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
    
    String url  = getURL(collection, id, "_update");
    
    try {
      // Abilitare script.inline: true in config/elasticsearch.yml
      int iIndexOf  = 0;
      int iBegin    = 0;
      iIndexOf      = fields.indexOf(',');
      while(iIndexOf >= 0) {
        http("POST", url, "{\"script\":\"ctx._source.remove(\\\"" + fields.substring(iBegin,iIndexOf) + "\\\")\"}");
        iIndexOf = fields.indexOf(',', iBegin=iIndexOf+1);
      }
      http("POST", url, "{\"script\":\"ctx._source.remove(\\\"" + fields.substring(iBegin) + "\\\")\"}");
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> 0");
      return 0;
    }
    
    if(debug) System.out.println(logprefix + "unset(" + collection + "," + fields + "," + id + ") -> 1");
    return 1;
  }
  
  @Override
  public
  int inc(String collection, String id, String field, Number value)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ")...");
    
    String url  = getURL(collection, id, "_update");
    
    try {
      // Abilitare script.inline: true in config/elasticsearch.yml
      http("POST", url, "{\"script\":{\"inline\":\"ctx._source." + field + " += " + value + "\"}}");
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> 0");
      return 0;
    }
    
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field + "," + value + ") -> 1");
    return 1;
  }
  
  @Override
  public
  int inc(String collection, String id, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    String url  = getURL(collection, id, "_update");
    
    try {
      // Abilitare script.inline: true in config/elasticsearch.yml
      http("POST", url, "{\"script\":{\"inline\":\"ctx._source." + field1 + " += " + value1 + "\"}}");
      http("POST", url, "{\"script\":{\"inline\":\"ctx._source." + field2 + " += " + value2 + "\"}}");
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> 0");
      return 0;
    }
    
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + id + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> 1");
    return 1;
  }
  
  @Override
  public
  int inc(String collection, Map<String,?> mapFilter, String field, Number value)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ")...");
    
    String url  = getSearchURL(collection, mapFilter, null, 0, "_source=false");
    
    WMap resGET = http("GET", url, null);
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> 0 (hits=null)");
      return 0;
    }
    
    int countUpd = 0;
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      
      Object _id = mapHit.get("_id");
      if(_id == null) continue;
      
      String urlU = getURL(collection, _id, "_update");
      
      try {
        // Abilitare script.inline: true in config/elasticsearch.yml
        http("POST", urlU, "{\"script\":{\"inline\":\"ctx._source." + field + " += " + value + "\"}}");
        countUpd++;
      }
      catch(Exception ex) {
        String message = ex.getMessage();
        if(message == null || !message.startsWith("{")) throw ex;
      }
    }
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field + "," + value + ") -> " + countUpd);
    return countUpd;
  }
  
  @Override
  public
  int inc(String collection, Map<String,?> mapFilter, String field1, Number value1, String field2, Number value2)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ")...");
    
    String url  = getSearchURL(collection, mapFilter, null, 0, "_source=false");
    
    WMap resGET = http("GET", url, null);
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> 0 (hits=null)");
      return 0;
    }
    
    int countUpd = 0;
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      
      Object _id = mapHit.get("_id");
      if(_id == null) continue;
      
      String urlU = getURL(collection, _id, "_update");
      
      try {
        // Abilitare script.inline: true in config/elasticsearch.yml
        http("POST", urlU, "{\"script\":{\"inline\":\"ctx._source." + field1 + " += " + value1 + "\"}}");
        http("POST", urlU, "{\"script\":{\"inline\":\"ctx._source." + field2 + " += " + value2 + "\"}}");
        countUpd++;
      }
      catch(Exception ex) {
        String message = ex.getMessage();
        if(message == null || !message.startsWith("{")) throw ex;
      }
    }
    if(debug) System.out.println(logprefix + "inc(" + collection + "," + mapFilter + "," + field1 + "," + value1 + "," + field2 + "," + value2 + ") -> " + countUpd);
    return countUpd;
  }
  
  @Override
  public
  int delete(String collection, String id)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ")...");
    
    if(collection == null || id == null) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> -1");
      return -1;
    }
    if(collection.equals("!")) collection = null;
    if(id.equals("!")) id = null;
    
    String urlDEL = getURL(collection, id);
    
    WMap resDEL = http("DELETE", urlDEL, null);
    
    if(resDEL.getBoolean("found")) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> 1");
      return 1;
    }
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + id + ") -> 0 (found=false)");
    return 0;
  }
  
  @Override
  public
  int delete(String collection, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ")...");
    
    if(collection == null || mapFilter == null) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> -1");
      return -1;
    }
    
    String url = getSearchURL(collection, mapFilter, null, 0, "_source=false");
    
    if(debug) System.out.println(logprefix + "delete GET " + dec(url));
    
    WMap resGET = http("GET", url, null);
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> 0 (hits=null)");
      return 0;
    }
    
    int countDel = 0;
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      Object _id = mapHit.get("_id");
      if(_id == null) continue;
      
      String urlDEL = getURL(collection, _id);
      
      WMap resDEL = http("DELETE", urlDEL, null);
      
      if(resDEL.getBoolean("found")) countDel++;
    }
    if(debug) System.out.println(logprefix + "delete(" + collection + "," + mapFilter + ") -> " + countDel);
    return countDel;
  }
  
  @Override
  public
  List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\")...");
    
    String[] toexcl = null;
    String  pfields = null;
    if(mapFilter != null) {
      toexcl = WUtil.toArrayOfString(mapFilter.get(FILTER_EXCLUDE), false);
      if(fields == null || fields.length() == 0 || fields.equals("*")) {
        pfields = WUtil.toString(mapFilter.get(FILTER_FIELDS), null);
      }
      else {
        pfields = fields;
      }
    }
    else
    if(fields != null && fields.length() > 0 && !fields.equals("*")) {
      pfields = fields;
    }
    if(pfields != null && pfields.length() > 0) {
      pfields = pfields.replace("[", "").replace("]", "").replace("\"", "").replace("'", "").replace(" ", "");
      if(pfields.length() > 0) pfields = "_source=" + pfields;
    }
    
    boolean addId = fields == null || fields.length() == 0 || fields.indexOf("_id") >= 0;
    
    String url  = getSearchURL(collection, mapFilter, pfields);
    
    WMap resGET = null;
    try {
      resGET = http("GET", url, null);
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      return new ArrayList<Map<String,Object>> (0);
    }
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ") -> 0 documents (hits=null)");
      return new ArrayList(0);
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> (listHits.size());
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      Object _id = mapHit.get("_id");
      Object src = mapHit.get("_source");
      if(src instanceof Map) {
        Map mapSource = (Map) src;
        if(addId) mapSource.put("_id", _id);
        if(toexcl != null) {
          for(int j = 0; j < toexcl.length; j++) {
            mapSource.remove(toexcl[j]);
          }
        }
        listResult.add(mapSource);
      }
      else {
        Object fld = mapHit.get("fields");
        if(fld instanceof Map) {
          Map mapFields = (Map) fld;
          Map mapRecord = new HashMap(mapFields.size()+1);
          Iterator iterator = mapFields.entrySet().iterator();
          while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            Object value = entry.getValue();
            if(value instanceof List) {
              List listValue = (List) value;
              if(listValue.size() > 0) {
                mapRecord.put(entry.getKey(), listValue.get(0));
              }
            }
            else {
              mapRecord.put(entry.getKey(), value);
            }
          }
          if(addId) mapRecord.put("_id", _id);
          listResult.add(mapRecord);
        }
      }
    }
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\") -> " + listResult.size() + " documents");
    return listResult;
  }
  
  public
  List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields, String orderBy, int limit)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + ",\"" + fields + "\",\"" + orderBy + "\"," + limit + ")...");
    
    String[] toexcl = null;
    String  pfields = null;
    if(mapFilter != null) {
      toexcl = WUtil.toArrayOfString(mapFilter.get(FILTER_EXCLUDE), false);
      if(fields == null || fields.length() == 0 || fields.equals("*")) {
        pfields = WUtil.toString(mapFilter.get(FILTER_FIELDS), null);
      }
      else {
        pfields = fields;
      }
    }
    else
    if(fields != null && fields.length() > 0 && !fields.equals("*")) {
      pfields = fields;
    }
    if(pfields != null && pfields.length() > 0) {
      pfields = pfields.replace("[", "").replace("]", "").replace("\"", "").replace("'", "").replace(" ", "");
      if(pfields.length() > 0) pfields = "_source=" + pfields;
    }
    
    boolean addId = fields == null || fields.length() == 0 || fields.indexOf("_id") >= 0;
    
    String url  = getSearchURL(collection, mapFilter, orderBy, limit, pfields);
    
    WMap resGET = null;
    try {
      resGET = http("GET", url, null);
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      return Collections.EMPTY_LIST;
    }
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "find(" + collection + "," + mapFilter + "," + orderBy + "," + limit + ") -> 0 documents (hits=null)");
      return Collections.EMPTY_LIST;
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> (listHits.size());
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      Object _id = mapHit.get("_id");
      Object src = mapHit.get("_source");
      if(src instanceof Map) {
        Map mapSource = (Map) src;
        if(addId) mapSource.put("_id", _id);
        if(toexcl != null) {
          for(int j = 0; j < toexcl.length; j++) {
            mapSource.remove(toexcl[j]);
          }
        }
        listResult.add(mapSource);
      }
      else {
        Object fld = mapHit.get("fields");
        if(fld instanceof Map) {
          Map mapFields = (Map) fld;
          Map mapRecord = new HashMap(mapFields.size()+1);
          Iterator iterator = mapFields.entrySet().iterator();
          while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            Object value = entry.getValue();
            if(value instanceof List) {
              List listValue = (List) value;
              if(listValue.size() > 0) {
                mapRecord.put(entry.getKey(), listValue.get(0));
              }
            }
            else {
              mapRecord.put(entry.getKey(), value);
            }
          }
          if(addId) mapRecord.put("_id", _id);
          listResult.add(mapRecord);
        }
      }
    }
    if(orderBy != null && orderBy.length() > 0) {
      String key = null;
      int sep = orderBy.indexOf(',');
      key = sep > 0 ? orderBy.substring(0, sep) : orderBy;
      String type  = getOrderType(key);
      boolean desc = type != null && type.equals(":desc");
      key = getOrderField(key);
      int iFirst = 0;
      int iLast  = listResult.size() - 1;
      boolean boSorted = true;
      do {
        for(int i = iLast; i > iFirst; i--) {
          Map<String,Object> m1 = listResult.get(i);
          Map<String,Object> m2 = listResult.get(i - 1);
          Object o1  = m1.get(key);
          Object o2  = m2.get(key);
          boolean lt = false;
          if(o1 instanceof Comparable && o2 instanceof Comparable) {
            lt = ((Comparable) o1).compareTo((Comparable) o2) < 0;
          }
          else {
            lt = o1 == null && o2 != null;
          }
          if(lt) {
            listResult.set(i,   m2);
            listResult.set(i-1, m1);
            boSorted = false;
          }
        }
        iFirst++;
      }
      while((iLast > iFirst) &&(!boSorted));
      if(desc) {
        listResult = reverse(listResult);
      }
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
    
    String url  = getSearchURL(collection, null, null);
    
    String data = "{\"query\":{\"match\":{\"" + field + "\":\"" + text.replace("\"", "\\\"") + "\"}}}";
    
    WMap resGET = null;
    try {
      resGET = http("GET", url, data);
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      return Collections.EMPTY_LIST;
    }
    
    WMap hits = new WMap(resGET.getMap("hits"));
    List listHits = hits.getList("hits");
    if(listHits == null) {
      if(debug) System.out.println(logprefix + "search(" + collection + "," + field + "," + text + ") -> 0 documents (hits=null)");
      return Collections.EMPTY_LIST;
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> (listHits.size());
    for(int i = 0; i < listHits.size(); i++) {
      Map mapHit = (Map) listHits.get(i);
      Object _id = mapHit.get("_id");
      Object src = mapHit.get("_source");
      if(src instanceof Map) {
        Map mapSource = (Map) src;
        mapSource.put("_id", _id);
        listResult.add(mapSource);
      }
      else {
        Object fld = mapHit.get("fields");
        if(fld instanceof Map) {
          Map mapFields = (Map) fld;
          Map mapRecord = new HashMap(mapFields.size()+1);
          Iterator iterator = mapFields.entrySet().iterator();
          while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            Object value = entry.getValue();
            if(value instanceof List) {
              List listValue = (List) value;
              if(listValue.size() > 0) {
                mapRecord.put(entry.getKey(), listValue.get(0));
              }
            }
            else {
              mapRecord.put(entry.getKey(), value);
            }
          }
          mapRecord.put("_id", _id);
          listResult.add(mapRecord);
        }
      }
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
    
    String alias = "";
    String funct = "";
    String argum = "";
    String afnc  = "";
    if(groupFunction != null && groupFunction.length() > 0 && !groupFunction.startsWith("c") && !groupFunction.startsWith("C")) {
      int iSep = groupFunction.indexOf('(');
      if(iSep > 0) {
        alias = groupFunction.substring(0,iSep).toLowerCase();
        funct = groupFunction.substring(0,iSep).toLowerCase();
        argum = groupFunction.substring(iSep+1,groupFunction.length()-1);
        afnc = ",\"aggs\":{\"" + alias + "\":{\"" + funct + "\":{\"field\":\"" + argum + "\"}}}";
      }
    }
    
    String data = "{\"aggs\":{\"group_by_" + field + "\":{\"terms\":{\"field\":\"" + field + "\"}" + afnc + "}}}";
    
    String url  = getSearchURL(collection, mapFilter, "size=0");
    
    WMap resGET = null;
    try {
      resGET = http("GET", url, data);
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      return new ArrayList<Map<String,Object>> (0);
    }
    
    WMap aggregations = new WMap(resGET.getMap("aggregations"));
    WMap groupByField = new WMap(aggregations.getMap("group_by_" + field));
    List buckets = groupByField.getList("buckets");
    if(buckets == null) {
      if(debug) System.out.println(logprefix + "group(" + collection + "," + mapFilter + ",\"" + field + "\",\"" + groupFunction + "\") -> 0 documents (buckets=null)");
      return new ArrayList<Map<String,Object>> (0);
    }
    
    List<Map<String,Object>> listResult = new ArrayList<Map<String,Object>> ();
    for(int i = 0; i < buckets.size(); i++) {
      Object oItem = buckets.get(i);
      if(oItem instanceof Map) {
        Map mapItem = (Map) oItem;
        Map<String,Object> mapRecord = new HashMap<String,Object> (2);
        
        Object oKey = mapItem.get("key");
        if(oKey != null) mapRecord.put(field, oKey);
        
        Object oAlias = mapItem.get(alias);
        if(oAlias instanceof Map) {
          Map mapAlias = (Map) oAlias;
          Object oValue = mapAlias.get("value");
          if(oValue != null) mapRecord.put("value", oValue);
        }
        else
        if(oAlias != null) {
          mapRecord.put("value", oAlias);
        }
        Object oDocCount = mapItem.get("doc_count");
        if(oDocCount != null) {
          if(funct == null || funct.length() == 0) {
            mapRecord.put("value", oDocCount);
          }
        }
        listResult.add(mapRecord);
      }
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
    
    String url  = getURL(collection, id);
    
    WMap resGET = http("GET", url, null);
    
    Map mapSource = resGET.getMap("_source");
    if(mapSource == null) {
      if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> null");
      return null;
    }
    
    mapSource.put("_id", id);
    if(debug) System.out.println(logprefix + "read(" + collection + "," + id + ") -> {" + mapSource.size() + "}");
    return mapSource;
  }
  
  @Override
  public
  int count(String collection, Map<String,?> mapFilter)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ")...");
    
    String url = getSearchURL(collection, mapFilter, "search_type=count");
    
    WMap resGET = null;
    try {
      resGET = http("GET", url, null);
    }
    catch(Exception ex) {
      String message = ex.getMessage();
      if(message == null || !message.startsWith("{")) {
        throw ex;
      }
      return 0;
    }
    
    WMap hits = new WMap(resGET.getMap("hits"));
    int result = hits.getInt("total");
    
    if(debug) System.out.println(logprefix + "count(" + collection + "," + mapFilter + ") -> " + result);
    return result;
  }
  
  @Override
  public
  boolean createIndex(String collection, String field, int type)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "createIndex(" + collection + "," + field + "," + type + ")...");
    boolean result = true;
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
    
    int length = content != null ? content.length : 0;
    Map<String,Object> mapDocument = new HashMap<String,Object> ();
    if(mapMetadata != null) {
      mapDocument.putAll(mapMetadata);
    }
    mapDocument.put(FILE_NAME,         filename);
    mapDocument.put(FILE_CONTENT,      content);
    mapDocument.put(FILE_LENGTH,       length);
    mapDocument.put(FILE_DATE_UPLOAD,  new java.util.Date());
    mapDocument.put(FILE_MD5,          getDigestMD5(content));
    
    String id = insert("files", mapDocument, false);
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
    
    Map<String,Object> mapFilter = new HashMap<String,Object> ();
    if(mapMetadata != null) {
      mapFilter.putAll(mapMetadata);
    }
    if(filename != null) {
      mapFilter.put(FILE_NAME, filename.replace('*', '%'));
    }
    
    List<Map<String,Object>> listResult = find("files", mapFilter, FILE_NAME + "," + FILE_LENGTH + "," + FILE_DATE_UPLOAD + "," + FILE_MD5);
    if(listResult == null) {
      if(debug) System.out.println(logprefix + "findFiles(" + filename + "," + mapMetadata + ") -> null");
      return null;
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
    
    Map mapFilter = new HashMap(1);
    mapFilter.put(FILE_NAME, filename);
    
    List<Map<String,Object>> listFindResult = find("files", mapFilter, "*");
    if(listFindResult == null) {
      if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> null (listFindResult=null)");
      return null;
    }
    if(listFindResult.size() == 0) {
      if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> null (listFindResult.size()=0)");
      return null;
    }
    Map<String,Object> map0 = (Map) listFindResult.get(0);
    if(debug) System.out.println(logprefix + "readFile(" + filename + ") -> {" + map0.size() + "}");
    return map0;
  }
  
  @Override
  public
  boolean removeFile(String filename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ")...");
    
    Map mapFilter = new HashMap(1);
    mapFilter.put(FILE_NAME, filename);
    
    int count = delete("files", mapFilter);
    boolean result = count > 0;
    if(debug) System.out.println(logprefix + "removeFile(" + filename + ") -> " + result);
    return result;
  }
  
  @Override
  public
  boolean renameFile(String filename, String newFilename)
    throws Exception
  {
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ")...");
    
    Map mapFilter = new HashMap(1);
    mapFilter.put(FILE_NAME, filename);
    
    Map mapData = new HashMap(1);
    mapData.put(FILE_NAME, newFilename);
    
    int count = update("files", mapData, mapFilter);
    boolean result = count > 0;
    if(debug) System.out.println(logprefix + "renameFile(" + filename + "," + newFilename + ") -> " + result);
    return result;
  }
  
  public
  String generateId()
  {
    byte arrayOfByte[] = new byte[12];
    ByteBuffer bb = ByteBuffer.wrap( arrayOfByte );
    bb.putInt((int)(System.currentTimeMillis() / 1000) );
    bb.putInt( _genmachine );
    bb.putInt( _nextInc.getAndIncrement() );
    final StringBuilder buf = new StringBuilder(24);
    for(final byte b : arrayOfByte) {
      buf.append(String.format("%02x", b & 0xff));
    }
    return buf.toString();
  }
  
  protected
  String getURL(String type, Object id)
  {
    if(type == null || type.length() == 0) {
      return "http://" + host + ":" + port + "/" + index + "/";
    }
    else
    if(type.equals("..")) {
      return "http://" + host + ":" + port;
    }
    if(id != null) {
      return "http://" + host + ":" + port + "/" + index + "/" + type + "/" + id;
    }
    return "http://" + host + ":" + port + "/" + index + "/" + type + "/";
  }
  
  protected
  String getURL(String type, Object id, String op)
  {
    if(type == null || type.length() == 0) {
      if(op != null && op.length() > 0) {
        return "http://" + host + ":" + port + "/" + index + "/" + op;
      }
      return "http://" + host + ":" + port + "/" + index + "/";
    }
    if(id != null) {
      if(op != null && op.length() > 0) {
        return "http://" + host + ":" + port + "/" + index + "/" + type + "/" + id + "/" + op;
      }
      return "http://" + host + ":" + port + "/" + index + "/" + type + "/" + id;
    }
    if(op != null && op.length() > 0) {
      return "http://" + host + ":" + port + "/" + index + "/" + type + "/" + op;
    }
    return "http://" + host + ":" + port + "/" + index + "/" + type + "/";
  }
  
  protected
  String getSearchURL(String type, Map mapFilter)
  {
    return getSearchURL(type, mapFilter, null, 0, null);
  }
  
  protected
  String getSearchURL(String type, Map mapFilter, String options)
  {
    return getSearchURL(type, mapFilter, null, 0, options);
  }
  
  protected
  String getSearchURL(String type, Map mapFilter, String orderBy, int limit, String options)
  {
    String sQuery = "?";
    if(options == null || options.length() == 0) {
      if(limit < 1) limit = defLimit;
      sQuery += "size=" + limit;
    }
    else {
      if(options.startsWith("fields")) {
        if(limit < 1) limit = defLimit;
        sQuery += "size=" + limit + "&" + options;
      }
      else
      if(limit > 0) {
        sQuery += "size=" + limit + "&" + options;
      }
      else {
        sQuery += options;
      }
    }
    // Si rimuove la clausola di ordinamento poiche' in elasticsearch di default
    // i valori stringa sono indicizzati come "analyzed".
    // "The problem with sorting on an analyzed field is not that it uses an analyzer,
    //  but that the analyzer tokenizes the string value into multiple tokens,
    //  like a bag of words, and Elasticsearch doesn't know which token to use for sorting."
    //    if(orderBy != null && orderBy.length() > 0) {
    //      sQuery += "&sort=";
    //      int iIndexOf = 0;
    //      int iBegin   = 0;
    //      iIndexOf     = orderBy.indexOf(',');
    //      while(iIndexOf >= 0) {
    //        String sOrderClause = orderBy.substring(iBegin, iIndexOf).trim();
    //        sQuery  += getOrderField(sOrderClause) + getOrderType(sOrderClause) + ",";
    //        iBegin   = iIndexOf + 1;
    //        iIndexOf = orderBy.indexOf(',', iBegin);
    //      }
    //      String sOrderClause = orderBy.substring(iBegin).trim();
    //      sQuery  += getOrderField(sOrderClause) + getOrderType(sOrderClause);
    //    }
    if(mapFilter == null) {
      return "http://" + host + ":" + port + "/" + index + "/" + type + "/_search" + sQuery;
    }
    
    Set<String> setOfKey = new HashSet<String> ();
    StringBuilder sbQ    = new StringBuilder();
    Iterator iterator = mapFilter.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey = entry.getKey();
      Object oVal = entry.getValue();
      String sKey = oKey.toString();
      
      if(sKey.equals(FILTER_EXCLUDE) || sKey.equals(FILTER_FIELDS)) continue;
      
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
      
      if(setOfKey.contains(sKey)) continue;
      setOfKey.add(sKey);
      
      String sVal = queryValue(oVal);
      if(sVal != null && !(boGTE || boLTE || boNE || boGT || boLT)) {
        boGTE  = sVal.startsWith(">=");
        boLTE  = sVal.startsWith("<=");
        boNE   = sVal.startsWith("<>");
        if(!boNE) boNE = sVal.startsWith("!=");
        if(boGTE || boLTE || boNE) sVal = sVal.substring(2);
        
        boGT   = sVal.startsWith(">");
        boLT   = sVal.startsWith("<");
        if(boGT || boLT) sVal = sVal.substring(1);
        
        if(boGTE || boLTE || boNE || boGT || boLT) {
          sVal = queryValue(JSON.parse(sVal));
        }
      }
      
      if(sVal.startsWith("%")) {
        sVal = sVal.substring(1);
        boStartsWithPerc = true;
      }
      if(sVal.endsWith("%")) {
        sVal = sVal.substring(0, sVal.length()-1);
        boEndsWithPerc = true;
      }
      
      if(sbQ.length() > 0) {
        sbQ.append(enc(" AND "));
      }
      
      if(sVal.equals("null")) {
        if(boNE) {
          sbQ.append("_exists_:" + enc(sKey));
        }
        else {
          sbQ.append("_missing_:" + enc(sKey));
        }
        continue;
      }
      
      if(boNE) {
        sbQ.append(enc("NOT " + sKey) + ":" + enc(sVal.replace("/", "\\/")));
      }
      else
      if(boGT) {
        String b = "]";
        Object oVal2 = findVal(mapFilter, "<=", sKey);
        if(oVal2 == null) {
          oVal2 = findVal(mapFilter, "<", sKey);
          b = "}";
        }
        if(oVal2 != null) {
          String sVal2 = queryValue(oVal2);
          sbQ.append(enc(sKey) + ":" + enc("{" + sVal + " TO " + sVal2 + b));
        }
        else {
          sbQ.append(enc(sKey) + ":" + enc(">" + sVal));
        }
      }
      else
      if(boLT) {
        String b = "[";
        Object oVal2 = findVal(mapFilter, ">=", sKey);
        if(oVal2 == null) {
          oVal2 = findVal(mapFilter, ">", sKey);
          b = "{";
        }
        if(oVal2 != null) {
          String sVal2 = queryValue(oVal2);
          sbQ.append(enc(sKey) + ":" + enc(b + sVal2 + " TO " + sVal + "}"));
        }
        else {
          sbQ.append(enc(sKey) + ":" + enc("<" + sVal));
        }
      }
      else
      if(boGTE) {
        String b = "]";
        Object oVal2 = findVal(mapFilter, "<=", sKey);
        if(oVal2 == null) {
          oVal2 = findVal(mapFilter, "<", sKey);
          b = "}";
        }
        if(oVal2 != null) {
          String sVal2 = queryValue(oVal2);
          sbQ.append(enc(sKey) + ":" + enc("[" + sVal + " TO " + sVal2 + b));
        }
        else {
          sbQ.append(enc(sKey) + ":" + enc(">=" + sVal));
        }
      }
      else
      if(boLTE) {
        String b = "[";
        Object oVal2 = findVal(mapFilter, ">=", sKey);
        if(oVal2 == null) {
          oVal2 = findVal(mapFilter, ">", sKey);
          b = "{";
        }
        if(oVal2 != null) {
          String sVal2 = queryValue(oVal2);
          sbQ.append(enc(sKey) + ":" + enc(b + sVal2 + " TO " + sVal + "]"));
        }
        else {
          sbQ.append(enc(sKey) + ":" + enc("<=" + sVal));
        }
      }
      else {
        if(boStartsWithPerc || boEndsWithPerc) {
          if(boStartsWithPerc && boEndsWithPerc) {
            sbQ.append(enc(sKey) + ":" + enc("/.*" + sVal.replace("/", "\\/").replace(",", "").replace("%", ".*") + ".*/"));
          }
          else
          if(boStartsWithPerc) {
            sbQ.append(enc(sKey) + ":" + enc("/.*" + sVal.replace("/", "\\/").replace(",", "").replace("%", ".*") + "/"));
          }
          else
          if(boEndsWithPerc) {
            sbQ.append(enc(sKey) + ":" + enc("/" + sVal.replace("/", "\\/").replace(",", "").replace("%", ".*") + ".*/"));
          }
        }
        else {
          sbQ.append(enc(sKey) + ":" + enc(sVal.replace("/", "\\/")));
        }
      }
    }
    if(sbQ.length() > 0) {
      if(setOfKey.size() == 1) {
        sQuery += "&q=" + sbQ;
      }
      else {
        sQuery += "&q=(" + sbQ + ")";
      }
    }
    return "http://" + host + ":" + port + "/" + index + "/" + type + "/_search" + sQuery;
  }
  
  protected static
  String getOrderField(String sOrderClause)
  {
    int iSep = sOrderClause.indexOf(' ');
    if(iSep > 0) return sOrderClause.substring(0, iSep);
    return sOrderClause;
  }
  
  protected static
  String getOrderType(String sOrderClause)
  {
    int iSep = sOrderClause.indexOf(' ');
    if(iSep > 0) {
      char c0 = sOrderClause.substring(iSep+1).charAt(0);
      if(c0 == 'd' || c0 == 'D' || c0 == '-') return ":desc";
      return ":asc";
    }
    return "";
  }
  
  protected
  WMap http(String method, String url, String data)
    throws Exception
  {
    if(debug) {
      String sampleData = null;
      if(data != null) {
        sampleData = data.length() > 500 ? data.substring(0, 500).replace('\n', ' ') + "...}" : data.replace('\n', ' ');
      }
      System.out.println(logprefix + "http(" + method + "," + dec(url) + "," + sampleData + ")...");
    }
    
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    
    if(method == null || method.length() < 2) method = "GET";
    connection.setRequestMethod(method.toUpperCase());
    if(user != null && user.length() > 0) {
      connection.addRequestProperty("Authorization", "Basic " + Base64Coder.encodeString(user + ":" + pass));
    }
    if(data != null) {
      connection.setDoOutput(true);
    }
    if(timeOut > 0) {
      connection.setConnectTimeout(timeOut);
      connection.setReadTimeout(timeOut);
    }
    
    int statusCode = 0;
    boolean error  = false;
    OutputStream out = null;
    try {
      if(data != null) {
        out = connection.getOutputStream();
        out.write(data.getBytes("UTF-8"));
        out.flush();
        out.close();
      }
      statusCode = connection.getResponseCode();
      error = statusCode >= 400;
    }
    catch(Exception ex) {
      String sampleData = null;
      if(data != null) {
        sampleData = data.length() > 500 ? data.substring(0, 500).replace('\n', ' ') + "...}" : data.replace('\n', ' ');
      }
      if(debug) System.out.println(logprefix + "http(" + method + "," + dec(url) + "," + sampleData + ") -> " + statusCode + " " + ex);
      throw ex;
    }
    finally {
      if(out != null) try{ out.close(); } catch(Exception ex) {}
    }
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      BufferedInputStream  bin = new BufferedInputStream(error ? connection.getErrorStream() : connection.getInputStream());
      byte[] buff = new byte[1024];
      int n;
      while((n = bin.read(buff)) > 0) baos.write(buff, 0, n);
      baos.flush();
      baos.close();
    }
    finally {
      if(connection != null) try{ connection.disconnect(); } catch(Exception ex) {}
    }
    
    byte[] abResponse = new String(baos.toByteArray(), "UTF-8").getBytes();
    
    String result = new String(abResponse);
    
    if(error) {
      if(debug) {
        String sampleData = null;
        if(data != null) {
          sampleData = data.length() > 500 ? data.substring(0, 500).replace('\n', ' ') + "...}" : data.replace('\n', ' ');
        }
        System.out.println(logprefix + "http(" + method + "," + dec(url) + "," + sampleData + ") -> " + statusCode + " Exception(" + result + ")");
      }
      throw new Exception(result);
    }
    if(result == null || !result.startsWith("{")) {
      if(debug) {
        String sampleData = null;
        if(data != null) {
          sampleData = data.length() > 500 ? data.substring(0, 500).replace('\n', ' ') + "...}" : data.replace('\n', ' ');
        }
        System.out.println(logprefix + "http(" + method + "," + dec(url) + "," + sampleData + ") -> " + statusCode + " Exception(Bad response: " + result + ")");
      }
      throw new Exception("Bad response: " + result);
    }
    Object oResult = JSON.parse(result);
    if(oResult instanceof Map) {
      if(debug) {
        String sampleData = null;
        if(data != null) {
          sampleData = data.length() > 500 ? data.substring(0, 500).replace('\n', ' ') + "...}" : data.replace('\n', ' ');
        }
        String sample = result.length() > 500 ? result.substring(0, 500).replace('\n', ' ') + "...}" : result.replace('\n', ' ');
        System.out.println(logprefix + "http(" + method + "," + dec(url) + "," + sampleData + ") -> " + statusCode + " " + sample);
      }
      return  new WMap((Map) oResult);
    }
    if(debug) {
      String sampleData = null;
      if(data != null) {
        sampleData = data.length() > 500 ? data.substring(0, 500).replace('\n', ' ') + "...}" : data.replace('\n', ' ');
      }
      System.out.println(logprefix + "http(" + method + "," + dec(url) + "," + sampleData + ") -> " + statusCode + " Exception(Bad response: " + result + ")");
    }
    throw new Exception("Bad response: " + result);
  }
  
  protected static
  String queryValue(Object oVal)
  {
    String sVal = null;
    if(oVal instanceof java.util.Date) {
      sVal = WUtil.toISO8601Timestamp_Z(oVal);
    }
    else
    if(oVal instanceof java.util.Calendar) {
      sVal = WUtil.toISO8601Timestamp_Z(oVal);
    }
    else
    if(oVal instanceof java.util.Collection) {
      sVal = "";
      Iterator it = ((java.util.Collection) oVal).iterator();
      while(it.hasNext()) sVal += " OR " + it.next();
      sVal = sVal.length() > 0 ? "(" + sVal.substring(4) + ")" : sVal;
    }
    else
    if(oVal != null && oVal.getClass().isArray()) {
      int arrayLength = Array.getLength(oVal);
      sVal = "";
      for(int i = 0; i < arrayLength; i++) {
        sVal += " OR " + Array.get(oVal, i);
      }
      sVal = sVal.length() > 0 ? "(" + sVal.substring(4) + ")" : sVal;
    }
    else {
      sVal = oVal != null ? oVal.toString() : "null";
    }
    return sVal;
  }
  
  protected static
  String enc(String s)
  {
    if(s == null) return "null";
    try { return URLEncoder.encode(s, "UTF-8"); } catch(Throwable ignore) {}
    return s;
  }
  
  protected static
  String dec(String s)
  {
    if(s == null) return "null";
    try { return URLDecoder.decode(s, "UTF-8"); } catch(Throwable ignore) {}
    return s;
  }
  
  protected static
  List reverse(List list)
  {
    if(list == null) return null;
    List listResult = new ArrayList(list.size());
    for(int i=list.size()-1; i >= 0; i--) {
      listResult.add(list.get(i));
    }
    return listResult;
  }
  
  protected static
  Object findVal(Map mapFilter, String sStarstWith, String sEndsWith)
  {
    Iterator iterator = mapFilter.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey = entry.getKey();
      String sKey = oKey.toString();
      if(!sKey.startsWith(sStarstWith)) continue;
      if(!sKey.endsWith(sEndsWith))     continue;
      return entry.getValue();
    }
    return null;
  }
  
  protected static
  String getDigestMD5(byte[] content)
    throws Exception
  {
    if(content == null) return "";
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(content);
      return String.valueOf(Base64Coder.encode(md.digest()));
    }
    catch(Exception ex) {
    }
    return "-";
  }
  
  private static AtomicInteger _nextInc = new AtomicInteger((new java.util.Random()).nextInt());
  private static final int _genmachine;
  static {
    try {
      // build a 2-byte machine piece based on NICs info
      int machinePiece;
      {
        try {
          StringBuilder sb = new StringBuilder();
          Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
          while( e.hasMoreElements() ) {
            NetworkInterface ni = e.nextElement();
            sb.append( ni.toString() );
          }
          machinePiece = sb.toString().hashCode() << 16;
        } catch(Throwable e) {
          machinePiece = (new Random().nextInt()) << 16;
        }
      }
      // add a 2 byte process piece. It must represent not only the JVM but the class loader.
      // Since static var belong to class loader there could be collisions otherwise
      final int processPiece;
      {
        int processId = new java.util.Random().nextInt();
        try {
          processId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
        }
        catch(Throwable t) {
        }
        ClassLoader loader = NoSQLElasticsearch.class.getClassLoader();
        int loaderId = loader != null ? System.identityHashCode(loader) : 0;
        
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(processId));
        sb.append(Integer.toHexString(loaderId));
        processPiece = sb.toString().hashCode() & 0xFFFF;
      }
      _genmachine = machinePiece | processPiece;
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
