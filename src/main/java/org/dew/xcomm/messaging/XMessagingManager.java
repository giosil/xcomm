package org.dew.xcomm.messaging;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.jivesoftware.smack.packet.XMPPError;

import org.dew.xcomm.nosql.INoSQLDB;

import org.dew.xcomm.util.ConnectionManager;
import org.dew.xcomm.util.WUtil;

public 
class XMessagingManager 
{
  protected static Logger logger = Logger.getLogger(XMessagingManager.class);
  
  public static
  XMessage insert(XMessage message)
    throws Exception
  {
    try {
      if(message == null) throw new Exception("message is null");
      
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      Map<String,Object> mapValues = WUtil.beanToMap(message);
      
      String id = noSQLDB.insert("Message", mapValues);
      
      message.setId(id);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.insert", ex);
    }
    return message;
  }
  
  public static
  List<XMessage> find(XMessage messageFilter)
    throws Exception
  {
    logger.debug("XMessagingManager.find(" + messageFilter + ")...");
    List<XMessage> listResult = null;
    try {
      if(messageFilter == null) {
        logger.debug("XMessagingManager.find(" + messageFilter + ") -> 0 items");
        return new ArrayList<XMessage>(0);
      }
      // Il thread deve essere sempre esplicitato: '*' in caso di tutti
      String thread = messageFilter.getThread();
      if(thread == null || thread.length() == 0) {
        logger.debug("XMessagingManager.find(" + messageFilter + ") -> 0 items (thread="+ thread + ")");
        return new ArrayList<XMessage>(0);
      }
      if(thread.equals("*")) messageFilter.setThread(null);
      
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      Map<String,Object> mapFilter = WUtil.beanToMap(messageFilter);
      
      List<Map<String,Object>> listFindResult = noSQLDB.find("Message", mapFilter, null);
      
      if(listFindResult != null) {
        listResult = new ArrayList<XMessage>(listFindResult.size());
        
        for(int i = 0; i < listFindResult.size(); i++) {
          
          Map<String,Object> mapMessage = listFindResult.get(i);
          
          XMessage message = WUtil.populateBean(XMessage.class, mapMessage);
          
          if(message == null) {
            logger.debug("   [" + i + "] message is null");
            continue;
          }
          
          if(WUtil.isBlank(message.getPayload())) {
            logger.debug("   [" + i + "] payload is blank: " + mapMessage);
            continue;
          }
          
          listResult.add(message);
        }
      }
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.find", ex);
      throw ex;
    }
    if(listResult == null) listResult = new ArrayList<XMessage>(0);
    logger.debug("XMessagingManager.find(" + messageFilter + ") -> " + WUtil.size(listResult) + " items");
    return listResult;
  }
  
  public static
  String reportRequest(XMessage message)
    throws Exception
  {
    if(message == null) return null;
    
    String to = message.getTo();
    if(to == null || to.length() == 0) {
      return null;
    }
    
    String thread = message.getThread();
    if(thread == null) thread = "";
    Date dateTime = message.getCreation();
    if(dateTime == null) dateTime = new Date();
    Integer type  = message.getPayloadType();
    
    Map<String,Object> mapData = new HashMap<String,Object>(3);
    mapData.put("username",        to);
    mapData.put("last_req",        dateTime);
    mapData.put("last_req_thread", thread);
    mapData.put("last_req_type",   type);
    
    Map<String,Object> mapFilter = new HashMap<String,Object>(1);
    mapFilter.put("username",      to);
    
    String result = null;
    try {
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      result = noSQLDB.upsert("Report", mapData, mapFilter);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.reportRequest", ex);
    }
    return result;
  }
  
  public static
  String reportResponse(XMessage message)
    throws Exception
  {
    if(message == null) return null;
    
    String from = message.getFrom();
    if(from == null || from.length() == 0) {
      return null;
    }
    
    String thread = message.getThread();
    if(thread == null) thread = "";
    Date dateTime = message.getUpdate();
    if(dateTime == null) dateTime = new Date();
    Integer type  = message.getPayloadType();
    
    Map<String,Object> mapData = new HashMap<String,Object>(3);
    mapData.put("username",        from);
    mapData.put("last_res",        dateTime);
    mapData.put("last_res_thread", thread);
    mapData.put("last_res_type",   type);
    
    Map<String,Object> mapFilter = new HashMap<String,Object>(1);
    mapFilter.put("username",      from);
    
    String result = null;
    try {
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      result = noSQLDB.upsert("Report", mapData, mapFilter);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.reportResponse", ex);
    }
    return result;
  }
  
  public static
  String reportError(XMessage message, XMPPError xmppError)
    throws Exception
  {
    if(message == null) return null;
    
    String from = message.getFrom();
    if(from == null || from.length() == 0) {
      return null;
    }
    
    String thread = message.getThread();
    if(thread == null) thread = "";
    Date dateTime = message.getUpdate();
    if(dateTime == null) dateTime = new Date();
    Integer type  = message.getPayloadType();
    
    String errorMessage = "";
    if(xmppError != null) {
      errorMessage = xmppError.getCondition() + " - " + xmppError.getType();
    }
    
    Map<String,Object> mapData = new HashMap<String,Object>(3);
    mapData.put("username",        from);
    mapData.put("last_err",        dateTime);
    mapData.put("last_err_thread", thread);
    mapData.put("last_err_type",   type);
    mapData.put("last_err_msg",    errorMessage);
    
    Map<String,Object> mapFilter = new HashMap<String,Object>(1);
    mapFilter.put("username",      from);
    
    String result = null;
    try {
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      result = noSQLDB.upsert("Report", mapData, mapFilter);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.reportError", ex);
    }
    return result;
  }
  
  public static
  String reportVoiceRequest(XMessage message)
    throws Exception
  {
    if(message == null) return null;
    
    String from = message.getFrom();
    if(from == null || from.length() == 0) {
      return null;
    }
    
    String thread = message.getThread();
    if(thread == null) thread = "";
    Date dateTime = message.getUpdate();
    if(dateTime == null) dateTime = new Date();
    
    Map<String,Object> mapData = new HashMap<String,Object>(3);
    mapData.put("username",          from);
    mapData.put("last_voice",        dateTime);
    mapData.put("last_voice_thread", thread);
    
    Map<String,Object> mapFilter = new HashMap<String,Object>(1);
    mapFilter.put("username",      from);
    
    String result = null;
    try {
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      result = noSQLDB.upsert("Report", mapData, mapFilter);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.reportVoiceRequest", ex);
    }
    return result;
  }
  
  public static
  boolean insertRejected(XMessage message)
    throws Exception
  {
    try {
      if(message == null) return false;
      
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      Map<String,Object> mapValues = WUtil.beanToMap(message);
      
      String id = noSQLDB.insert("Rejected", mapValues);
      
      message.setId(id);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.insertRejected", ex);
    }
    return true;
  }
  
  public static
  boolean canReceive(String username, Integer payloadType)
  {
    if(username == null || username.length() == 0) {
      return false;
    }
    username = username.trim().toLowerCase();
    
    Map<String,Object> mapFilter = new HashMap<String,Object>(1);
    mapFilter.put("username", username);
    
    List<Map<String,Object>> resFind = null;
    try {
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      resFind = noSQLDB.find("Report", mapFilter, null);
      
      if(resFind == null || resFind.size() == 0) {
        // Se la collezione non e' disponibile NON si puo' fare alcuna valutazione.
        // Si rende comunque disponibile di default l'utente.
        return true;
      }
      
      Map<String,Object> mapReport = resFind.get(0);
      
      Calendar calLastReq = WUtil.toCalendar(mapReport.get("last_req"),   null);
      Calendar calLastRes = WUtil.toCalendar(mapReport.get("last_res"),   null);
      Calendar calLastErr = WUtil.toCalendar(mapReport.get("last_err"),   null);
      Calendar calLastVoi = WUtil.toCalendar(mapReport.get("last_voice"), null);
      
      if(calLastReq == null) {
        // Se non e' stata mai registrata alcuna richiesta... 
        if(calLastVoi != null) {
          // ma e' stata registrata una richiesta di voice...
          return true;
        }
        else
        if(calLastRes != null) {
          // ma e' stato registrato un messaggio di risposta... (Improbabile se non in caso di drop collection)
          return true;
        }
        return false;
      }
      
      // calLastReq != null
      
      if(calLastRes != null && !calLastRes.before(calLastReq)) {
        // Se e' stata registrata una riposta posteriore alla richiesta...
        return true;
      }
      if(calLastVoi != null && !calLastVoi.before(calLastReq)) {
        // Se e' stata registrata una richiesta di voice posteriore alla richiesta...
        return true;
      }
      if(calLastErr != null && !calLastErr.before(calLastReq)) {
        // Se e' stato registrato un errore posteriore alla richiesta...
        return false;
      }
      
      // Se e' stata registrata una richiesta (calLastReq != null), ma non 
      // sono stati registrati errori, risposte o richieste di voice posteriori...
      boolean pushMessage = payloadType != null && payloadType.intValue() > 4;
      if(pushMessage) {
        // Nel caso di messaggi push si consente l'invio anche in assenza di una
        // risposta ad una precedente richiesta.
        return true;
      }
      
      if(calLastRes != null) {
        long lDiff = System.currentTimeMillis() - calLastRes.getTimeInMillis();
        if(lDiff > 900000) {
          // se questa situazione perdura da piu' di 15 minuti...
          return false;
        }
      }
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.canReceive", ex);
    }
    // Si reputa disponibile l'utente che comunque ha una coda di messaggi
    return true;
  }
  
  public static
  int removeUpTo(Date dUpTo)
  {
    if(dUpTo == null) return 0;
    int iResult = 0;
    Map<String,Object> mapFilter = new HashMap<String,Object>();
    mapFilter.put("creation<", dUpTo);
    try {
      INoSQLDB noSQLDB = ConnectionManager.getDefaultNoSQLDB();
      
      iResult = noSQLDB.delete("Message", mapFilter);
      
      noSQLDB.delete("Rejected", mapFilter);
    }
    catch(Exception ex) {
      logger.error("Eccezione in XMessagingManager.removeUpTo", ex);
    }
    return iResult;
  }
}
