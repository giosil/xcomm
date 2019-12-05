package org.dew.xcomm.ws;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.dew.xcomm.nosql.util.WUtil;

import org.dew.xcomm.messaging.XMessage;
import org.dew.xcomm.messaging.XMessagingClient;
import org.dew.xcomm.messaging.XMessagingManager;

/**
 * Servizio per la gestione del sistema di messaggistica basato su XMPP. 
 */
public 
class WSMessages 
{
	protected static Logger logger = Logger.getLogger(WSMessages.class);
	
	public static final String VERSION = "1.0.0";
	
	/**
	 * Servizio per l'invio di un messaggio.
	 * 
	 * @param mapMessage Messaggio
	 * @return Token (thread) della conversazione
	 * @throws Exception Eccezione
	 */
	public static
	String post(Map<String,Object> mapMessage)
		throws Exception
	{
		logger.debug("WSMessages.post(" + mapMessage + ")...");
		
		if(mapMessage == null || mapMessage.isEmpty()) {
			logger.debug("WSMessages.post(" + mapMessage + ") -> \"\" (invalid mapMessage)");
			return "";
		}
		
		String result = null;
		try {
			XMessage message = WUtil.populateBean(XMessage.class, mapMessage);
			
			XMessagingClient client = XMessagingClient.getInstance();
			
			if(!XMessagingManager.canReceive(message.getTo(), message.getPayloadType())) {
				logger.debug("WSMessages.post(" + mapMessage + ") -> \"\" (" + message.getTo() + " can't receive)");
				if(WUtil.isBlank(message.getFrom())) {
					message.setFrom(client.getUser());
				}
				if(WUtil.isBlank(message.getCreation())) {
					message.setCreation(new Date());
				}
				if(WUtil.isBlank(message.getType())) {
					message.setType(XMessage.DEFAULT_OUTBOUND_TYPE);
				}
				XMessagingManager.insertRejected(message);
				return "";
			}
			
			message = client.sendMessage(message);
			
			result = message != null ? message.getThread() : null;
			
			XMessagingManager.reportRequest(message);
		}
		catch(Exception ex) {
			logger.error("Eccezione in WSMessages.post(" + mapMessage + ")", ex);
			throw ex;
		}
		logger.debug("WSMessages.post(" + mapMessage + ") -> result");
		return result;
	}
	
	/**
	 * Servizio per la lettura dei messaggi ricevuti
	 * 
	 * @param options opzioni di ricerca (ad es. all)
	 * @param mapFilter Filtro sui messaggi (generalmente per thread)
	 * @return Lista di messaggi archiviati.
	 * @throws Exception Eccezione
	 */
	public static
	List<XMessage> find(String options, Map<String,Object> mapFilter)
		throws Exception
	{
		return find(mapFilter);
	}
	
	/**
	 * Servizio per la lettura dei messaggi ricevuti
	 * 
	 * @param mapFilter Filtro sui messaggi (generalmente per thread)
	 * @return Lista di messaggi archiviati.
	 * @throws Exception Eccezione
	 */
	public static
	List<XMessage> find(Map<String,Object> mapFilter)
		throws Exception
	{
		if(mapFilter == null || mapFilter.isEmpty()) {
			logger.debug("WSMessages.find(" + mapFilter + ") -> 0 messages");
			return new ArrayList<XMessage>(0);
		}
		
		try {
			XMessage messageFilter = WUtil.populateBean(XMessage.class, mapFilter);
			
			List<XMessage> listResult = XMessagingManager.find(messageFilter);
			
			logger.debug("WSMessages.find(" + mapFilter + ") -> " + WUtil.size(listResult) + " messages");
			return listResult;
		}
		catch(Exception ex) {
			logger.error("Eccezione in WSMessages.find(" + mapFilter + ")", ex);
			throw ex;
		}
	}
	
	/**
	 * Servizio per la creazione di un account.
	 * 
	 * @param username Nome utente
	 * @param password Password
	 * @return true se la creazione dell'account e' andata a buon fine.
	 * @throws Exception Eccezione
	 */
	public static
	boolean createAccount(String username, String password)
		throws Exception
	{
		boolean result = false;
		try {
			XMessagingClient client = XMessagingClient.getInstance();
			
			result = client.createAccount(username, password);
		}
		catch(Exception ex) {
			logger.error("Eccezione in WSMessages.createAccount(" + username + "," + password + ")", ex);
			throw ex;
		}
		logger.debug("WSMessages.createAccount(" + username + "," + password + ") -> " + result);
		return result;
	}
	
	/**
	 * Servizio che consente di ottenere la lista degli utenti censiti.
	 * 
	 * @return List&lt;String&gt; Lista utenti censiti.
	 * @throws Exception Eccezione
	 */
	public static
	List<String> listAccounts()
		throws Exception
	{
		List<String> result = null;
		try {
			XMessagingClient client = XMessagingClient.getInstance();
			
			result = client.listAccounts();
		}
		catch(Exception ex) {
			logger.error("Eccezione in WSMessages.listAccounts()", ex);
			throw ex;
		}
		logger.debug("WSMessages.listAccounts() -> " + WUtil.size(result) + " items");
		return result;
	}
	
	/**
	 * Servizio che consente di verificare se l'utente e' in grado di ricevere messaggi.
	 * 
	 * @param username Utente
	 * @return boolean true = l'utente puo' ricevere messaggi, false = altrimenti
	 * @throws Exception Eccezione
	 */
	public static
	boolean canReceive(String username)
		throws Exception
	{
		boolean result = false;
		try {
			result = XMessagingManager.canReceive(username, null);
		}
		catch(Exception ex) {
			logger.error("Eccezione in WSMessages.canReceive(" + username + ")", ex);
			throw ex;
		}
		logger.debug("WSMessages.canReceive(" + username + ") -> " + result);
		return result;
	}
	
	/**
	 * Servizio che consente di verificare se l'utente e' in grado di ricevere messaggi.
	 * 
	 * @param username Utente
	 * @param payloadType Tipo payload
	 * @return boolean true = l'utente puo' ricevere messaggi, false = altrimenti
	 * @throws Exception Eccezione
	 */
	public static
	boolean canReceive(String username, Integer payloadType)
		throws Exception
	{
		boolean result = false;
		try {
			result = XMessagingManager.canReceive(username, payloadType);
		}
		catch(Exception ex) {
			logger.error("Eccezione in WSMessages.canReceive(" + username + ")", ex);
			throw ex;
		}
		logger.debug("WSMessages.canReceive(" + username + ") -> " + result);
		return result;
	}
}
