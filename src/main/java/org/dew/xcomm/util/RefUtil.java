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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;

@SuppressWarnings({"rawtypes","unchecked"})
public
class RefUtil
{
  // Restituisce il tipo con generici SOLO in presenza di bean.
  // 1.5+
  public static
  String getBeanGenericType(Method method, int iIndex)
  {
    Type[] genericTypes = method.getGenericParameterTypes();
    if(genericTypes == null || genericTypes.length <= iIndex) {
      return null;
    }
    Type type = genericTypes[iIndex];
    if(type instanceof ParameterizedType) {
      String sResult = type.toString();
      if(sResult == null) return null;
      int iSepG = sResult.indexOf('<');
      if(iSepG > 0) {
        String g1 = "";
        String g2 = "";
        String generics = sResult.substring(iSepG + 1, sResult.length()-1);
        int iSepT = generics.indexOf(',');
        if(iSepT > 0) {
          g1 = generics.substring(0,iSepT).trim();
          g2 = generics.substring(iSepT+1).trim();
        }
        else {
          g1 = generics;
        }
        if(g1 != null && g1.length() > 0 && !g1.startsWith("java.") && !g1.startsWith("[") && !g1.startsWith("?")) {
          return sResult;
        }
        if(g2 != null && g2.length() > 0 && !g2.startsWith("java.") && !g2.startsWith("[") && !g2.startsWith("?")) {
          return sResult;
        }
      }
    }
    return null;
  }
  
  public static
  Object[] getParametersExt(Method method, List params)
  {
    Class[] types = method.getParameterTypes();
    if(types.length != params.size()) return null;
    Object[] aoResult = new Object[types.length];
    for(int i = 0; i < types.length; i++) {
      String sTypeName = types[i].getName();
      Object param     = params.get(i);
      
      if(sTypeName.equals("java.lang.String")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toString(param, null);
        }
      }
      else
      if(sTypeName.equals("int")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = new Integer(0);
        }
        else {
          aoResult[i] = WUtil.toInteger(param, new Integer(0));
        }
      }
      else
      if(sTypeName.equals("java.lang.Number")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toNumber(param, null);
        }
      }
      else
      if(sTypeName.equals("java.lang.Integer")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toInteger(param, null);
        }
      }
      else
      if(sTypeName.equals("long")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = new Long(0);
        }
        else {
          aoResult[i] = WUtil.toLongObj(param, new Long(0));
        }
      }
      else
      if(sTypeName.equals("java.lang.Long")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toLongObj(param, null);
        }
      }
      else
      if(sTypeName.equals("double")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = new Double(0.0d);
        }
        else {
          aoResult[i] = WUtil.toDoubleObj(param, new Double(0.0d));
        }
      }
      else
      if(sTypeName.equals("java.lang.Double")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toDoubleObj(param, null);
        }
      }
      else
      if(sTypeName.equals("java.math.BigDecimal")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toBigDecimal(param, null);
        }
      }
      else
      if(sTypeName.equals("java.math.BigInteger")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toBigInteger(param, null);
        }
      }
      else
      if(sTypeName.equals("boolean")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = Boolean.FALSE;
        }
        else {
          aoResult[i] = WUtil.toBooleanObj(param, Boolean.FALSE);
        }
      }
      else
      if(sTypeName.equals("java.lang.Boolean")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toBooleanObj(param, null);
        }
      }
      else
      if(sTypeName.equals("java.util.Date")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toDate(param, null);
        }
      }
      else
      if(sTypeName.equals("java.util.Calendar")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toCalendar(param, null);
        }
      }
      else // 1.8+
      if(sTypeName.equals("java.time.LocalDate")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toLocalDate(param, null);
        }
      }
      else // 1.8+
      if(sTypeName.equals("java.time.LocalDateTime")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toLocalDateTime(param, null);
        }
      }
      else
      if(sTypeName.equals("java.util.Vector")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = WUtil.toVector(param, null);
          }
        }
      }
      else
      if(sTypeName.equals("java.util.Stack")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            Stack stack = new Stack();
            stack.addAll(WUtil.toVector(param, true));
            aoResult[i] = stack;
          }
        }
      }
      else
      if(sTypeName.equals("java.util.List") || sTypeName.equals("java.util.ArrayList") || sTypeName.equals("java.util.Collection")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = WUtil.toList(param, null);
          }
        }
      }
      else
      if(sTypeName.equals("java.util.LinkedList")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = new LinkedList(WUtil.toList(param, true));
          }
        }
      }
      else
      if(sTypeName.equals("java.util.Hashtable")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = WUtil.toHashtable(param, false);
          }
        }
      }
      else
      if(sTypeName.equals("java.util.Map") || sTypeName.equals("java.util.HashMap")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = WUtil.toMap(param, false);
          }
        }
      }
      else
      if(sTypeName.equals("java.util.Properties")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof Map) {
          Properties properties = new Properties();
          Iterator iterator = ((Map) param).entrySet().iterator();
          while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            Object oVal = entry.getValue();
            if(oVal == null) continue;
            properties.setProperty(entry.getKey().toString(), oVal.toString());
          }
          aoResult[i] = properties;
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.equals("java.util.TreeSet")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = new TreeSet(WUtil.toList(param, true));
          }
        }
      }
      else
      if(sTypeName.equals("java.util.TreeMap")) {
        String sBeanGenericType = getBeanGenericType(method, i);
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          if(sBeanGenericType != null) {
            aoResult[i] = WUtil.toObject(param, sBeanGenericType);
          }
          else {
            aoResult[i] = new TreeMap(WUtil.toMap(param, true));
          }
        }
      }
      else
      if(sTypeName.equals("[B")) { // byte[]
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof byte[]) {
          aoResult[i] = param;
        }
        else
        if(param instanceof Collection) {
          byte[] array = Arrays.toArrayOfByte((Collection) param);
          if(array == null) return null;
          aoResult[i] = array;
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.equals("[I")) { // int[]
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof int[]) {
          aoResult[i] = param;
        }
        else
        if(param instanceof Collection) {
          int[] array = Arrays.toArrayOfInt((Collection) param);
          if(array == null) return null;
          aoResult[i] = array;
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.equals("[C")) { // char[]
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof char[]) {
          aoResult[i] = param;
        }
        else
        if(param instanceof Collection) {
          char[] array = Arrays.toArrayOfChar((Collection) param);
          if(array == null) return null;
          aoResult[i] = array;
        }
        else
        if(param instanceof String) {
          String sParam = (String) param;
          aoResult[i] = sParam.toCharArray();
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.equals("[Z")) { // boolean[]
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof boolean[]) {
          aoResult[i] = param;
        }
        else
        if(param instanceof Collection) {
          boolean[] array = Arrays.toArrayOfBoolean((Collection) param);
          if(array == null) return null;
          aoResult[i] = array;
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.equals("[D")) { // double[]
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof double[]) {
          aoResult[i] = param;
        }
        else
        if(param instanceof Collection) {
          double[] array = Arrays.toArrayOfDouble((Collection) param);
          if(array == null) return null;
          aoResult[i] = array;
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.startsWith("[L") && sTypeName.endsWith(";")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else 
        if(param.getClass().isArray()) {
          aoResult[i] = WUtil.toObject(param, sTypeName);
        }
        else
        if(param instanceof Collection) {
          aoResult[i] = WUtil.toObject(param, sTypeName);
        }
        else {
          return null;
        }
      }
      else
      if(sTypeName.equals("java.util.Set") || sTypeName.equals("java.util.HashSet")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toSet(param, false);
        }
      }
      else
      if(sTypeName.equals("java.sql.Date")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toSQLDate(param, null);
        }
      }
      else
      if(sTypeName.equals("java.sql.Time")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toSQLTime(param, null);
        }
      }
      else
      if(sTypeName.equals("java.sql.Timestamp")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = WUtil.toSQLTimestamp(param, null);
        }
      }
      else
      if(sTypeName.equals("java.lang.Object")) {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else {
          aoResult[i] = param;
        }
      }
      else {
        if(param == null || param.equals("null")) {
          aoResult[i] = null;
        }
        else
        if(param instanceof Map) {
          aoResult[i] = WUtil.populateBean(types[i], (Map) param);
        }
        else 
        if(param instanceof String) {
          if(types[i].isEnum()) {
            try {
              aoResult[i] = Enum.valueOf(types[i], (String) param);
            }
            catch(Exception ex) {
              System.err.println("RpcUtil.getParametersExt(" + method + "," + params + "): " + ex);
              return null;
            }
          }
          else {
            return null;
          }
        }
        else {
          return null;
        }
      }
    }
    return aoResult;
  }
}
