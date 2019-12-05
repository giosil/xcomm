package org.dew.xcomm.nosql;

import java.util.List;
import java.util.Map;

public
interface INoSQLDB
{
  public static final String FILTER_EXCLUDE   = "-";
  public static final String FILTER_FIELDS    = "*";
  
  public static final String FILE_CONTENT     = "content";
  public static final String FILE_NAME        = "filename";
  public static final String FILE_LENGTH      = "length";
  public static final String FILE_DATE_UPLOAD = "uploadDate";
  public static final String FILE_MD5         = "md5";
  
  public void setDebug(boolean debug);
  
  public boolean isDebug();
  
  public Map<String,Object> getInfo() throws Exception;
  
  public List<String> getCollections() throws Exception;
  
  public boolean drop(String collection) throws Exception;
  
  public String insert(String collection, Map<String,?> mapData) throws Exception;
  
  public String insert(String collection, Map<String,?> mapData, boolean refresh) throws Exception;
  
  public int bulkIns(String collection, List<Map<String,?>> listData) throws Exception;
  
  public boolean replace(String collection, Map<String,?> mapData, String id) throws Exception;
  
  public int update(String collection, Map<String,?> mapData, String id) throws Exception;
  
  public int update(String collection, Map<String,?> mapData, Map<String,?> mapFilter) throws Exception;
  
  public String upsert(String collection, Map<String,?> mapData, Map<String,?> mapFilter) throws Exception;
  
  public int unset(String collection, String fields, String id) throws Exception;
  
  public int inc(String collection, String id, String field, Number value) throws Exception;
  
  public int inc(String collection, String id, String field1, Number value1, String field2, Number value2) throws Exception;
  
  public int inc(String collection, Map<String,?> mapFilter, String field, Number value) throws Exception;
  
  public int inc(String collection, Map<String,?> mapFilter, String field1, Number value1, String field2, Number value2) throws Exception;
  
  public int delete(String collection, String id) throws Exception;
  
  public int delete(String collection, Map<String,?> mapFilter) throws Exception;
  
  public List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields) throws Exception;
  
  public List<Map<String,Object>> find(String collection, Map<String,?> mapFilter, String fields, String orderBy, int limit) throws Exception;
  
  public List<Map<String,Object>> search(String collection, String field, String text) throws Exception;
  
  public List<Map<String,Object>> group(String collection, Map<String,?> mapFilter, String field, String groupFunction) throws Exception;
  
  public Map<String,Object> read(String collection, String id) throws Exception;
  
  public int count(String collection, Map<String,?> mapFilter) throws Exception;
  
  public boolean createIndex(String collection, String field, int type) throws Exception;
  
  public List<Map<String,Object>> listIndexes(String collection) throws Exception;
  
  public String writeFile(String filename, byte[] content, Map<String,?> mapMetadata) throws Exception;
  
  public List<Map<String,Object>> findFiles(String filename, Map<String,?> mapMetadata) throws Exception;
  
  public Map<String,Object> readFile(String filename) throws Exception;
  
  public boolean removeFile(String filename) throws Exception;
  
  public boolean renameFile(String filename, String newFilename) throws Exception;
}
