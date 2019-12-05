package org.dew.xcomm.messaging;

import java.io.Serializable;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.dew.xcomm.nosql.json.JSON;

import org.dew.xcomm.util.WMap;
import org.dew.xcomm.util.WUtil;

public 
class XMessage implements Serializable
{
	private static final long serialVersionUID = -2594835917378068089L;
	
	public static final String DEFAULT_CHANNEL_TYPE      = "Unicast";
	public static final String DEFAULT_INBOUND_TYPE      = "Response";
	public static final String DEFAULT_INBOUND_CATEGORY  = "GenericResponse";
	public static final String DEFAULT_OUTBOUND_TYPE     = "Request";
	public static final String DEFAULT_OUTBOUND_CATEGORY = "GenericRequest";
	
	// Attributi di base
	protected String  id;
	protected Date    creation;
	protected Date    update;
	protected String  from;
	protected String  to;
	
	// Attributi applicativi
	protected String  thread;
	protected String  channelType;
	protected String  category;
	protected String  type;
	protected Integer payloadType;
	protected String  payload;
	
	// Attributi di sistema
	protected Boolean success;
	protected Boolean sent;
	protected String  error;
	
	public XMessage()
	{
	}
	
	@SuppressWarnings("rawtypes")
	public XMessage(String body)
	{
		if(body == null || body.length() == 0) {
			return;
		}
		if(body.indexOf("&quot;") >= 0) {
			body = WUtil.toEscapedText(body, "");
		}
		Object oBody = JSON.parse(normalizeText(body));
		if(!(oBody instanceof Map)) return;
		
		Date currentDateTime = new Date();
		WMap wmap = new WMap((Map) oBody);
		this.thread      = wmap.getString("thread");
		this.channelType = wmap.getString("channelType", DEFAULT_CHANNEL_TYPE);
		this.category    = wmap.getString("category",    DEFAULT_INBOUND_CATEGORY);
		this.type        = wmap.getString("type",        DEFAULT_INBOUND_TYPE);
		this.payloadType = wmap.getInteger("payloadType");
		this.payload     = wmap.getString("payload");
		this.creation    = wmap.getDate("creation", currentDateTime);
		this.update      = wmap.getDate("update",   currentDateTime);
		
		Boolean msgSucc  = wmap.getBooleanObj("success");
		if(payload != null && payload.length() > 2) {
			Map<String,Object> mapPayload = null;
			try {
				mapPayload = WUtil.toMapObject(JSON.parse(payload));
				if(mapPayload != null) {
					Boolean payloadSuccess = WUtil.toBooleanObj(mapPayload.get("success"), null);
					if(payloadSuccess != null) {
						this.success = payloadSuccess;
					}
					else {
						this.success = msgSucc != null ? msgSucc.booleanValue() : false;
					}
				}
				else {
					if(msgSucc != null) {
						this.payload = "{\"success\":" + msgSucc + "}";
						this.success = msgSucc.booleanValue();
					}
					else {
						this.success = Boolean.FALSE;
					}
				}
			}
			catch(Exception ex) {
				System.err.println("[XMessage] Exception during parse payload " + payload + ": " + ex);
			}
		}
		else 
		if(msgSucc != null) {
			this.payload = "{\"success\":" + msgSucc + "}";
			this.success = msgSucc;
		}
	}
	
	public XMessage(String type, String thread)
	{
		this.type = type;
		this.thread = thread;
	}
	
	public XMessage(String category, String type, String to, String payload)
	{
		this.category = category;
		this.type = type;
		this.to = to;
		this.payload = payload;
	}
	
	public XMessage(String category, String type, String to, Map<String,Object> mapPayload)
	{
		this.category = category;
		this.type = type;
		this.to = to;
		if(mapPayload != null && !mapPayload.isEmpty()) {
			String jsonObject = JSON.stringify(mapPayload);
			String stringJson = JSON.stringify(jsonObject);
			if(stringJson != null && stringJson.length() > 2) {
				this.payload = stringJson.substring(1, stringJson.length()-1);
			}
			else {
				this.payload = "{}";
			}
		}
		else {
			this.payload = "{}";
		}
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
	}

	public Date getUpdate() {
		return update;
	}

	public void setUpdate(Date update) {
		this.update = update;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getThread() {
		return thread;
	}

	public void setThread(String thread) {
		this.thread = thread;
	}

	public String getChannelType() {
		return channelType;
	}

	public void setChannelType(String channelType) {
		this.channelType = channelType;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Integer getPayloadType() {
		return payloadType;
	}

	public void setPayloadType(Integer payloadType) {
		this.payloadType = payloadType;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public Boolean getSuccess() {
		return success;
	}

	public void setSuccess(Boolean success) {
		this.success = success;
	}

	public Boolean getSent() {
		return sent;
	}

	public void setSent(Boolean sent) {
		this.sent = sent;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
	
	protected static 
	String normalizeText(String sText) 
	{
		if (sText == null || sText.length() == 0) return "";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < sText.length(); i++) {
			char c = sText.charAt(i);
			if (c < 9)       sb.append("_");  else 
			if (c == '\340') sb.append("a'"); else 
			if (c == '\350') sb.append("e'"); else 
			if (c == '\354') sb.append("i'"); else 
			if (c == '\362') sb.append("o'"); else 
			if (c == '\371') sb.append("u'"); else 
			if (c == '\341') sb.append("a'"); else 
			if (c == '\351') sb.append("e'"); else 
			if (c == '\355') sb.append("i'"); else 
			if (c == '\363') sb.append("o'"); else 
			if (c == '\372') sb.append("u'"); else 
			if (c == '\300') sb.append("A'"); else 
			if (c == '\310') sb.append("E'"); else 
			if (c == '\314') sb.append("I'"); else 
			if (c == '\322') sb.append("O'"); else 
			if (c == '\331') sb.append("U'"); else 
			if (c == '\301') sb.append("A'"); else 
			if (c == '\311') sb.append("E'"); else 
			if (c == '\315') sb.append("I'"); else 
			if (c == '\323') sb.append("O'"); else 
			if (c == '\332') sb.append("U'"); else 
			if (c > 127)     sb.append("_"); 
			else sb.append(c);
		}
		return sb.toString();
	}
	
	@Override
	public int hashCode() {
		if(id == null) return 0;
		return id.hashCode();
	}
	
	@Override
	public boolean equals(Object object) {
		if(object instanceof XMessage) {
			String object_Id = ((XMessage) object).getId();
			if(id == null) return object_Id == null; 
			return id.equals(object_Id);
		}
		return false;
	}
	
	@Override
	public String toString() {
		Map<String,Object> mapValues = new HashMap<String,Object>();
		if(creation != null) {
			mapValues.put("creation", WUtil.toISO8601Timestamp_Offset(creation, ":"));
		}
		if(update != null) {
			mapValues.put("update", WUtil.toISO8601Timestamp_Offset(update, ":"));
		}
		if(to != null) to = to.toLowerCase().trim();
		mapValues.put("from",        from);
		mapValues.put("to",          to);
		mapValues.put("thread",      thread);
		mapValues.put("channelType", channelType);
		mapValues.put("category",    category);
		mapValues.put("type",        type);
		mapValues.put("payloadType", payloadType);
		mapValues.put("payload",     payload);
		return normalizeText(JSON.stringify(mapValues));
	}
}
