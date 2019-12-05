package org.dew.xcomm.util;

import java.util.*;
import java.io.*;

import java.util.Date;

import javax.naming.Context;
import javax.naming.InitialContext;

@SuppressWarnings({"rawtypes"})
public
class BEConfig
{
  public static Properties config = new Properties();
  
  private static boolean boConfigFileLoaded = false;
  private static String sResultLoading = "OK";
  
  static {
    String sUserHome = System.getProperty("user.home");
    String sPathFile = sUserHome + File.separator + "cfg" + File.separator + "xcomm_be.cfg";
    try {
      InputStream in = (InputStream)new FileInputStream(sPathFile);
      config = new Properties();
      config.load(in);
      in.close();
      boConfigFileLoaded = true;
      sResultLoading = "File " + sPathFile + " loaded.";
    }
    catch(FileNotFoundException ex) {
      sResultLoading = "File " + sPathFile + " not found.";
    }
    catch(IOException ioex) {
      sResultLoading = "IOException during load " + sPathFile + ": " + ioex;
    }
  }
  
  public static
  boolean isConfigFileLoaded()
  {
    return boConfigFileLoaded;
  }
  
  public static
  String getResultLoading()
  {
    return sResultLoading;
  }
  
  public static
  String lookup(String sKey, String sDefault)
    throws Exception
  {
    Context ctx = new InitialContext();
    Object lookup = ctx.lookup("java:global/" + sKey);
    if(lookup != null) {
      String result = lookup.toString();
      if(result == null || result.length() == 0) {
        return sDefault;
      }
      return result;
    }
    return sDefault;
  }
  
  public static
  String getProperty(String sKey)
  {
    try {
      return lookup(sKey, null);
    }
    catch(Exception ex) {
    }
    return config.getProperty(sKey);
  }
  
  public static
  String getProperty(String sKey, String sDefault)
  {
    try {
      return lookup(sKey, sDefault);
    }
    catch(Exception ex) {
    }
    return config.getProperty(sKey, sDefault);
  }
  
  public static
  String getProperty(String sKey, boolean boMandatory)
    throws Exception
  {
    String sResult = null;
    try {
      sResult = lookup(sKey, null);
    }
    catch(Exception ex) {
      sResult = config.getProperty(sKey, null);
    }
    if(boMandatory) {
      if(sResult == null || sResult.length() == 0) {
        throw new Exception("Entry \"" + sKey + "\" of configuration is blank.");
      }
    }
    return sResult;
  }
  
  public static
  Calendar getCalendarProperty(String sKey, Calendar oDefault)
  {
    String sValue = getProperty(sKey);
    return WUtil.toCalendar(sValue, oDefault);
  }
  
  public static
  Date getDateProperty(String sKey, Date oDefault)
  {
    String sValue = getProperty(sKey);
    return WUtil.toDate(sValue, oDefault);
  }
  
  public static
  List getListProperty(String sKey)
  {
    String sValue = getProperty(sKey);
    return WUtil.toList(sValue, true);
  }
  
  public static
  boolean getBooleanProperty(String sKey, boolean bDefault)
  {
    String sValue = getProperty(sKey);
    return WUtil.toBoolean(sValue, bDefault);
  }
  
  public static
  int getIntProperty(String sKey, int iDefault)
  {
    String sValue = getProperty(sKey);
    return WUtil.toInt(sValue, iDefault);
  }
  
  public static
  double getDoubleProperty(String sKey, double dDefault)
  {
    String sValue = getProperty(sKey);
    return WUtil.toDouble(sValue, dDefault);
  }
  
  public static
  String getDefaultDbName()
  {
    String sResult = getProperty("nosqldb.dbname");
    if(sResult != null && sResult.length() > 0) {
      return sResult;
    }
    sResult = getProperty("nosqldb.dbauth");
    if(sResult != null && sResult.length() > 0) {
      return sResult;
    }
    sResult = getProperty("nosqldb.uri");
    if(sResult == null || sResult.length() == 0) {
      sResult = getProperty("nosqldb.url");
    }
    String sDbName = "xcomm";
    int iLastSep = sResult.lastIndexOf('/');
    if(iLastSep > 0) {
      sDbName = sResult.substring(iLastSep + 1);
      int iSepOpt = sDbName.indexOf('?');
      if(iSepOpt > 0) sDbName = sDbName.substring(0, iSepOpt);
      if(sDbName.equalsIgnoreCase("admin")) sDbName = "xcomm";
    }
    return sDbName;
  }
}
