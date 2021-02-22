/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dew.xcomm.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.net.URLEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Wrapper Map
 */
@SuppressWarnings({"rawtypes","unchecked"})
public
class WMap implements Map
{
  protected Map map;
  
  public WMap()
  {
    map = new HashMap();
  }
  
  public WMap(boolean boLegacy)
  {
    if(boLegacy) {
      map = new Hashtable();
    }
    else {
      map = new HashMap();
    }
  }
  
  public WMap(int initialCapacity)
  {
    map = new HashMap(initialCapacity);
  }
  
  public WMap(int initialCapacity, float loadFactor)
  {
    map = new HashMap(initialCapacity, loadFactor);
  }
  
  public WMap(Map m)
  {
    map = m != null ? m : new HashMap();
  }
  
  public int size() {
    return map.size();
  }
  
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }
  
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }
  
  public Object get(Object key) {
    return map.get(key);
  }
  
  public Object put(Object key, Object value) {
    if(map instanceof Hashtable && value == null) {
      map.remove(key);
      return null;
    }
    return map.put(key, value);
  }
  
  public Object remove(Object key) {
    return map.remove(key);
  }
  
  public void putAll(Map m) {
    if(m == null) return;
    map.putAll(m);
  }
  
  public void clear() {
    map.clear();
  }
  
  public Set keySet() {
    return map.keySet();
  }
  
  public Collection values() {
    return map.values();
  }
  
  public Set entrySet() {
    return map.entrySet();
  }
  
  public int hashCode() {
    return map.hashCode();
  }
  
  public boolean equals(Object o) {
    return map.equals(o);
  }
  
  public String toString() {
    if(map == null) return "{}";
    return map.toString();
  }
  
  public Hashtable toHashtable() {
    if(map instanceof Hashtable) return (Hashtable) map;
    Hashtable hashtable = new Hashtable(map.size());
    Iterator iterator = map.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey = entry.getKey();
      Object oVal = entry.getValue();
      if(oVal != null) hashtable.put(oKey, oVal);
    }
    return hashtable;
  }
  
  public HashMap toHashMap() {
    if(map instanceof HashMap) return (HashMap) map;
    HashMap hashMap = new HashMap(map.size());
    Iterator iterator = map.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey = entry.getKey();
      Object oVal = entry.getValue();
      hashMap.put(oKey, oVal);
    }
    return hashMap;
  }
  
  public Map<String,Object> toMapObject() {
    return map;
  }
  
  public 
  Map<String,Object> toMapObject(boolean replaceNonStringKeys) {
    return WUtil.toMapObject(map, replaceNonStringKeys);
  }
  
  public String toQueryString() {
    StringBuffer sbResult = new StringBuffer();
    Iterator iterator = map.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      Object oKey = entry.getKey();
      Object oVal = entry.getValue();
      String sVal = WUtil.toString(oVal, "null");
      try { sbResult.append("&" + URLEncoder.encode(oKey.toString(), "UTF-8") + "=" + URLEncoder.encode(sVal, "UTF-8")); }
      catch(Throwable ignore) {}
    }
    if(sbResult.length() > 1) return sbResult.substring(1);
    return sbResult.toString();
  }
  
  // get methods
  
  public int getInt(Object key) {
    return WUtil.toInt(get(key), 0);
  }
  
  public int getInt(Object key, int iDefault) {
    return WUtil.toInt(get(key), iDefault);
  }
  
  public Integer getInteger(Object key) {
    return WUtil.toInteger(get(key), null);
  }
  
  public Integer getInteger(Object key, Integer oDefault) {
    return WUtil.toInteger(get(key), oDefault);
  }
  
  public double getDouble(Object key) {
    return WUtil.toDouble(get(key), 0.0d);
  }
  
  public double getDouble(Object key, double dDefault) {
    return WUtil.toDouble(get(key), dDefault);
  }
  
  public double getDouble(Object key, double dDefault, int decimal) {
    double dValue = WUtil.toDouble(get(key), dDefault);
    return WUtil.round(dValue, decimal);
  }
  
  public Double getDoubleObj(Object key) {
    return WUtil.toDoubleObj(get(key), null);
  }
  
  public Double getDoubleObj(Object key, Double oDefault) {
    return WUtil.toDoubleObj(get(key), oDefault);
  }
  
  public Double getDoubleObj(Object key, Double oDefault, int decimal) {
    Double oValue = WUtil.toDoubleObj(get(key), oDefault);
    if(oValue == null) return null;
    return new Double(WUtil.round(oValue.doubleValue(), decimal));
  }
  
  public long getLong(Object key) {
    return WUtil.toLong(get(key), 0);
  }
  
  public long getLong(Object key, long lDefault) {
    return WUtil.toLong(get(key), lDefault);
  }
  
  public Long getLongObj(Object key) {
    return WUtil.toLongObj(get(key), null);
  }
  
  public Long getLongObj(Object key, Long oDefault) {
    return WUtil.toLongObj(get(key), oDefault);
  }
  
  public BigDecimal getBigDecimal(Object key) {
    return WUtil.toBigDecimal(get(key), null);
  }
  
  public BigDecimal getBigDecimal(Object key, BigDecimal oDefault) {
    return WUtil.toBigDecimal(get(key), oDefault);
  }
  
  public BigInteger getBigInteger(Object key) {
    return WUtil.toBigInteger(get(key), null);
  }
  
  public BigInteger getBigInteger(Object key, BigInteger oDefault) {
    return WUtil.toBigInteger(get(key), oDefault);
  }
  
  public boolean getBoolean(Object key) {
    return WUtil.toBoolean(get(key), false);
  }
  
  public boolean getBoolean(Object key, boolean boDefault) {
    return WUtil.toBoolean(get(key), boDefault);
  }
  
  public Boolean getBooleanObj(Object key) {
    return WUtil.toBooleanObj(get(key), null);
  }
  
  public Boolean getBooleanObj(Object key, Boolean oDefault) {
    return WUtil.toBooleanObj(get(key), oDefault);
  }
  
  public String getString(Object key) {
    return WUtil.toString(get(key), null);
  }
  
  public String getString(Object key, int iMaxLength) {
    return WUtil.toString(get(key), null, iMaxLength);
  }
  
  public String getString(Object key, String sDefault) {
    return WUtil.toString(get(key), sDefault);
  }
  
  public String getString(Object key, String sDefault, int iMaxLength) {
    return WUtil.toString(get(key), sDefault, iMaxLength);
  }
  
  public String getUpperString(Object key) {
    return WUtil.toUpperString(get(key), null);
  }
  
  public String getUpperString(Object key, String sDefault) {
    return WUtil.toUpperString(get(key), sDefault);
  }
  
  public String getUpperString(Object key, String sDefault, int iMaxLength) {
    return WUtil.toUpperString(get(key), sDefault, iMaxLength);
  }
  
  public String getLowerString(Object key) {
    return WUtil.toLowerString(get(key), null);
  }
  
  public String getLowerString(Object key, String sDefault) {
    return WUtil.toLowerString(get(key), sDefault);
  }
  
  public String getLowerString(Object key, String sDefault, int iMaxLength) {
    return WUtil.toLowerString(get(key), sDefault, iMaxLength);
  }
  
  public String getEscapedText(Object key, String sDefault) {
    return WUtil.toEscapedText(get(key), sDefault);
  }
  
  public String getHTMLText(Object key, String sDefault) {
    return WUtil.toHTMLText(get(key), sDefault);
  }
  
  // 1.8+
  public java.time.LocalDate getLocalDate(Object key) {
    return WUtil.toLocalDate(get(key), null);
  }
  
  // 1.8+
  public java.time.LocalDate getLocalDate(Object key, Object oDefault) {
    return WUtil.toLocalDate(get(key), oDefault);
  }
  
  // 1.8+
  public java.time.LocalDateTime getLocalDateTime(Object key) {
    return WUtil.toLocalDateTime(get(key), null);
  }
  
  // 1.8+
  public java.time.LocalDateTime getLocalDateTime(Object key, Object oDefault) {
    return WUtil.toLocalDateTime(get(key), oDefault);
  }
  
  public java.util.Date getDate(Object key) {
    return WUtil.toDate(get(key), null);
  }
  
  public java.util.Date getDate(Object key, Object oDefault) {
    return WUtil.toDate(get(key), oDefault);
  }
  
  public java.util.Date getDate(Object key, Object oDefault, Object oTime) {
    java.util.Date result = WUtil.toDate(get(key), oDefault);
    return WUtil.setTime(result, oTime);
  }
  
  public java.util.Date getTime(Object key) {
    return WUtil.toTime(get(key), null);
  }
  
  public java.util.Date getTime(Object key, Object oDefault) {
    return WUtil.toTime(get(key), oDefault);
  }
  
  public java.util.Date getDateTime(Object keyDate, Object keyTime) {
    return WUtil.toTime(get(keyDate), get(keyTime), null);
  }
  
  public java.util.Date getDateTime(Object keyDate, Object keyTime, Object oDefault) {
    return WUtil.toTime(get(keyDate), get(keyTime), oDefault);
  }
  
  public java.util.Calendar getCalendar(Object key) {
    return WUtil.toCalendar(get(key), null);
  }
  
  public java.util.Calendar getCalendar(Object key, Object oDefault) {
    return WUtil.toCalendar(get(key), oDefault);
  }
  
  public java.util.Calendar getCalendar(Object key, Object oDefault, Object oTime) {
    java.util.Calendar result = WUtil.toCalendar(get(key), oDefault);
    return WUtil.setTime(result, oTime);
  }
  
  public java.sql.Date getSQLDate(Object key) {
    return WUtil.toSQLDate(get(key), null);
  }
  
  public java.sql.Date getSQLDate(Object key, Object oDefault) {
    return WUtil.toSQLDate(get(key), oDefault);
  }
  
  public java.sql.Date getSQLDate(Object key, Object oDefault, Object oTime) {
    java.sql.Date result = WUtil.toSQLDate(get(key), oDefault);
    return WUtil.setTime(result, oTime);
  }
  
  public java.sql.Time getSQLTime(Object key) {
    return WUtil.toSQLTime(get(key), null);
  }
  
  public java.sql.Time getSQLTime(Object key, Object oDefault) {
    return WUtil.toSQLTime(get(key), oDefault);
  }
  
  public java.sql.Timestamp getSQLTimestamp(Object key) {
    return WUtil.toSQLTimestamp(get(key), null);
  }
  
  public java.sql.Timestamp getSQLTimestamp(Object key, Object oDefault) {
    return WUtil.toSQLTimestamp(get(key), oDefault);
  }
  
  public java.sql.Timestamp getSQLTimestamp(Object key, Object oDefault, Object oTime) {
    java.sql.Timestamp result = WUtil.toSQLTimestamp(get(key), oDefault);
    return (java.sql.Timestamp) WUtil.setTime(result, oTime);
  }
  
  public int getIntDate(Object key) {
    return WUtil.toIntDate(get(key), 0);
  }
  
  public int getIntDate(Object key, int iDate) {
    return WUtil.toIntDate(get(key), iDate);
  }
  
  public int getIntTime(Object key) {
    return WUtil.toIntTime(get(key), 0);
  }
  
  public int getIntTime(Object key, int iTime) {
    return WUtil.toIntTime(get(key), iTime);
  }
  
  public String getStringTime(Object key) {
    return WUtil.toStringTime(get(key), null);
  }
  
  public String getStringTime(Object key, String sDefault) {
    return WUtil.toStringTime(get(key), sDefault);
  }
  
  public String getFormattedDate(Object key, Object locale) {
    return WUtil.formatDate(get(key), locale);
  }
  
  public String getFormattedTime(Object key, boolean second, boolean millis) {
    return WUtil.formatTime(get(key), second, millis);
  }
  
  public String getFormattedDateTime(Object key, Object locale, boolean second) {
    return WUtil.formatDateTime(get(key), locale, second);
  }
  
  public String getFormattedCurrency(Object key, Object locale, String left, String right) {
    return WUtil.formatCurrency(get(key), locale, left, right);
  }
  
  public Vector getVector(Object key) {
    return WUtil.toVector(get(key), false);
  }
  
  public Vector getVector(Object key, boolean notNull) {
    return WUtil.toVector(get(key), notNull);
  }
  
  public Vector getVector(Object key, Object oDefault) {
    return WUtil.toVector(get(key), oDefault);
  }
  
  public List getList(Object key) {
    return WUtil.toList(get(key), false);
  }
  
  public List getList(Object key, boolean notNull) {
    return WUtil.toList(get(key), notNull);
  }
  
  public List getList(Object key, Object oDefault) {
    return WUtil.toList(get(key), oDefault);
  }
  
  public <T> List<T> getList(Object key, Class<T> itemsClass, Object oDefault) {
    return WUtil.toList(get(key), itemsClass, oDefault);
  }
  
  public List<Map<String,Object>> getListOfMapObject(Object key) {
    return WUtil.toListOfMapObject(get(key));
  }
  
  public Hashtable getHashtable(Object key) {
    return WUtil.toHashtable(get(key), false);
  }
  
  public Hashtable getHashtable(Object key, boolean notNull) {
    return WUtil.toHashtable(get(key), notNull);
  }
  
  public Hashtable getHashtable(Object key, Object oDefault) {
    return WUtil.toHashtable(get(key), oDefault);
  }
  
  public Map getMap(Object key) {
    return WUtil.toMap(get(key), false);
  }
  
  public Map getMap(Object key, boolean notNull) {
    return WUtil.toMap(get(key), notNull);
  }
  
  public Map getMap(Object key, Object oDefault) {
    return WUtil.toMap(get(key), oDefault);
  }
  
  public Object getValue(Object key, Object subKey, Object oDefault) {
    return WUtil.getValue(get(key), subKey, oDefault);
  }
  
  public Map<String,Object> getMapObject(Object key) {
    return WUtil.toMapObject(get(key));
  }
  
  public <T> T getBean(Object key, Class<T> beanClass) {
    Object item = get(key);
    if(item == null) return null;
    if(beanClass.isAssignableFrom(item.getClass())) {
      return (T) item;
    }
    Map mapValues = WUtil.toMap(item, false);
    if(mapValues == null) return null;
    return WUtil.populateBean(beanClass, mapValues);
  }
  
  public Set getSet(Object key) {
    return WUtil.toSet(get(key), false);
  }
  
  public Set getSet(Object key, boolean notNull) {
    return WUtil.toSet(get(key), notNull);
  }
  
  public Set getSet(Object key, Object oDefault) {
    return WUtil.toSet(get(key), oDefault);
  }
  
  public String[] getArrayOfString(Object key) {
    return WUtil.toArrayOfString(get(key), false);
  }
  
  public String[] getArrayOfString(Object key, boolean notNull) {
    return WUtil.toArrayOfString(get(key), notNull);
  }
  
  public int[] getArrayOfInt(Object key) {
    return WUtil.toArrayOfInt(get(key), false);
  }
  
  public int[] getArrayOfInt(Object key, boolean notNull) {
    return WUtil.toArrayOfInt(get(key), notNull);
  }
  
  public double[] getArrayOfDouble(Object key) {
    return WUtil.toArrayOfDouble(get(key), false);
  }
  
  public double[] getArrayOfDouble(Object key, boolean notNull) {
    return WUtil.toArrayOfDouble(get(key), notNull);
  }
  
  public byte[] getArrayOfByte(Object key) {
    return WUtil.toArrayOfByte(get(key), false);
  }
  
  public byte[] getArrayOfByte(Object key, boolean notNull) {
    return WUtil.toArrayOfByte(get(key), notNull);
  }
  
  // put methods
  
  public Object put(Object key, int i) {
    return put(key, new Integer(i));
  }
  
  public Object put(Object key, long l) {
    return put(key, new Long(l));
  }
  
  public Object put(Object key, double d) {
    return put(key, new Double(d));
  }
  
  public Object put(Object key, double d, int decimal) {
    return put(key, new Double(WUtil.round(d, decimal)));
  }
  
  public Object put(Object key, boolean b) {
    return put(key, new Boolean(b));
  }
  
  public Object putBoolean(Object key, Object object) {
    return put(key, WUtil.toBooleanObj(object, null));
  }
  
  public Object putBoolean(Object key, Object object, boolean boDefault) {
    return put(key, WUtil.toBooleanObj(object, new Boolean(boDefault)));
  }
  
  public Object putString(Object key, Object object) {
    return put(key, WUtil.toString(object, null));
  }
  
  public Object putString(Object key, Object object, String sDefault) {
    return put(key, WUtil.toString(object, sDefault));
  }
  
  public Object putUpper(Object key, Object object) {
    return put(key, WUtil.toUpperString(object, null));
  }
  
  public Object putLower(Object key, Object object) {
    return put(key, WUtil.toLowerString(object, null));
  }
  
  public Object putDate(Object key, Object object) {
    return put(key, WUtil.toDate(object, null));
  }
  
  public Object putCalendar(Object key, Object object) {
    return put(key, WUtil.toCalendar(object, null));
  }
  
  public Object putTime(Object key, Object object) {
    return put(key, WUtil.toTime(object, null));
  }
  
  public Object putDateTime(Object key, Object date, Object time) {
    return put(key, WUtil.toTimeCalendar(date, time, null));
  }
  
  public Object putDate(Object key, int iYYYYMMDD) {
    return put(key, WUtil.toDate(iYYYYMMDD, 0));
  }
  
  public Object putCalendar(Object key, int iYYYYMMDD) {
    return put(key, WUtil.toCalendar(iYYYYMMDD, 0));
  }
  
  public Object putTime(Object key, int iTime) {
    return put(key, WUtil.intToTime(iTime));
  }
  
  public Object putNumber(Object key, Object object, Object oDefault) {
    return put(key, WUtil.toNumber(object, oDefault));
  }
  
  public Object putNumber(Object key, Object object, Object oDefault, int decimal) {
    Number number = WUtil.toNumber(object, oDefault);
    if(number != null) {
      double dValue = number.doubleValue();
      return put(key, new Double(WUtil.round(dValue, decimal)));
    }
    return put(key, null);
  }
  
  public Object putBigDecimal(Object key, Object object, BigDecimal oDefault) {
    return put(key, WUtil.toBigDecimal(object, oDefault));
  }
  
  public Object putList(Object key, int iId, String sDesc) {
    if(iId == 0 &&(sDesc == null || sDesc.length() == 0)) {
      remove(key);
      return null;
    }
    if(sDesc == null) sDesc = String.valueOf(iId);
    List list = new ArrayList(2);
    list.add(new Integer(iId));
    list.add(sDesc);
    return put(key, list);
  }
  
  public Object putList(Object key, int iId, String sCode, String sDesc) {
    if(iId == 0 &&(sCode == null || sCode.length() == 0)) {
      remove(key);
      return null;
    }
    if(sCode == null) sCode = String.valueOf(iId);
    if(sDesc == null) sDesc = sCode;
    List list = new ArrayList(3);
    list.add(new Integer(iId));
    list.add(sCode);
    list.add(sDesc);
    return put(key, list);
  }
  
  public Object putList(Object key, Object oId, String sDesc) {
    if(oId == null) {
      remove(key);
      return null;
    }
    if(sDesc == null) sDesc = oId.toString();
    List list = new ArrayList(2);
    list.add(oId);
    list.add(sDesc);
    return put(key, list);
  }
  
  public Object putList(Object key, Object oId, String sCode, String sDesc) {
    if(oId == null) {
      remove(key);
      return null;
    }
    if(sCode == null) sCode = oId.toString();
    if(sDesc == null) sDesc = sCode;
    List list = new ArrayList(3);
    list.add(oId);
    list.add(sCode);
    list.add(sDesc);
    return put(key, list);
  }
  
  public Object putNotNull(Object key, Object e) {
    if(e == null) {
      remove(key);
      return null;
    }
    return put(key, e);
  }
  
  public Object putNotZero(Object key, int i) {
    if(i == 0) {
      remove(key);
      return null;
    }
    return put(key, new Integer(i));
  }
  
  public Object putNotZero(Object key, int iId, String sCode) {
    if(iId == 0) {
      remove(key);
      return null;
    }
    return putList(key, iId, sCode);
  }
  
  public Object putNotZero(Object key, int iId, String sCode, String sDesc) {
    if(iId == 0) {
      remove(key);
      return null;
    }
    return putList(key, iId, sCode, sDesc);
  }
  
  public boolean isBlank(Object key) {
    return WUtil.isBlank(get(key));
  }
  
  public boolean isBlank(Object key, int iBlankValue) {
    return WUtil.isBlank(get(key), iBlankValue);
  }
}
