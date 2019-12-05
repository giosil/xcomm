package org.dew.xcomm.messaging;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import org.jivesoftware.smack.AbstractXMPPConnection;
import org.jivesoftware.smack.ConnectionConfiguration.SecurityMode;
import org.jivesoftware.smack.ConnectionListener;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.StanzaListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.chat2.Chat;
import org.jivesoftware.smack.chat2.ChatManager;
import org.jivesoftware.smack.filter.StanzaTypeFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Stanza;
import org.jivesoftware.smack.packet.XMPPError;
import org.jivesoftware.smack.roster.Roster;
import org.jivesoftware.smack.roster.RosterEntry;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;
import org.jivesoftware.smackx.iqregister.AccountManager;

import org.jxmpp.jid.BareJid;
import org.jxmpp.jid.EntityBareJid;
import org.jxmpp.jid.Jid;
import org.jxmpp.jid.impl.JidCreate;
import org.jxmpp.jid.parts.Localpart;

import org.dew.xcomm.nosql.util.WUtil;

import org.dew.xcomm.util.BEConfig;
import org.dew.xcomm.util.ConnectionManager;

public 
class XMessagingClient 
{
	protected String  host           = BEConfig.getProperty("messagingHost",              "localhost");
	protected String  user           = BEConfig.getProperty("messagingUser",              "xcomm");
	protected String  pass           = BEConfig.getProperty("messagingPass",              "XXXX");
	protected String  domain         = BEConfig.getProperty("messagingDomain",            "xcomm.dew.org");
	protected boolean supervisor     = BEConfig.getBooleanProperty("messagingSupervisor", false);
	protected String  supervisorUser = BEConfig.getProperty("messagingSupervisorUser",    "user");
	protected String  supervisorPass = BEConfig.getProperty("messagingSupervisorPass",    "XXXX");
	
	protected XMPPTCPConnectionConfiguration.Builder configBuilder;
	protected AbstractXMPPConnection connection;
	protected AccountManager accountManager;
	protected ChatManager chatManager;
	protected Roster roster;
	
	protected static Logger logger = Logger.getLogger(XMessagingClient.class);
	
	protected static XMessagingClient  singletonInstance;
	
	public static
	XMessagingClient getInstance()
		throws Exception
	{
		if(singletonInstance == null) {
			singletonInstance = new XMessagingClient();
			singletonInstance.connect();
		}
		return singletonInstance;
	}
	
	protected XMessagingClient()
		throws Exception
	{
		logger.debug("XMessagingClient()...");
		logger.debug("host=" + host);
		logger.debug("user=" + user + "@" + domain);
		try {
			logger.debug("configBuilder = XMPPTCPConnectionConfiguration.builder()...");
			configBuilder = XMPPTCPConnectionConfiguration.builder();
			if(ConnectionManager.boIsOnDebug) {
				logger.debug("configBuilder.setSecurityMode(SecurityMode.required)...");
				configBuilder.setSecurityMode(SecurityMode.required);
			}
			else {
				// In produzione il server e' configurato per ricevere messaggi in chiaro dalla rete interna.
				logger.debug("configBuilder.setSecurityMode(SecurityMode.disabled)...");
				configBuilder.setSecurityMode(SecurityMode.disabled);
			}
			configBuilder.setUsernameAndPassword(user, pass);
			configBuilder.setHost(host);
			configBuilder.setXmppDomain(JidCreate.domainBareFrom(domain));
			
			logger.debug("connection = new XMPPTCPConnection(configBuilder.build())...");
			connection = new XMPPTCPConnection(configBuilder.build());
			
			logger.debug("connection.addConnectionListener...");
			connection.addConnectionListener(new ConnectionListener() {
				public void reconnectionSuccessful() {
					logger.debug("ConnectionListener.reconnectionSuccessful");
				}
				public void reconnectionFailed(Exception exception) {
					logger.debug("ConnectionListener.reconnectionFailed(" + exception + ")");
				}
				public void reconnectingIn(int seconds) {
					logger.debug("ConnectionListener.reconnectingIn(" + seconds + ")");
				}
				public void connectionClosedOnError(Exception exception) {
					logger.error("ConnectionListener.connectionClosedOnError(" + exception + ")", exception);
				}
				public void connectionClosed() {
					logger.warn("ConnectionListener.connectionClosed()");
				}
				public void connected(XMPPConnection conn) {
					logger.debug("ConnectionListener.connected(" + conn + ")");
				}
				public void authenticated(XMPPConnection conn, boolean resumed) {
					logger.debug("ConnectionListener.authenticated(" + conn + "," + resumed + ")");
				}
			});
			
			logger.debug("connection.addAsyncStanzaListener...");
			connection.addAsyncStanzaListener(new StanzaListener() {
				public void processStanza(Stanza stanza) throws NotConnectedException, InterruptedException {
					logger.debug("processStanza(" + stanza + ")...");
					if (stanza instanceof Message) {
						Message message = Message.class.cast(stanza);
						String  body    = message.getBody();
						if(body != null && body.length() > 0) {
							Jid jidFrom = message.getFrom();
							Localpart localPart = jidFrom.getLocalpartOrNull();
							String from = localPart != null ? localPart.toString() : null;
							if(from == null || from.length() == 0) {
								from = jidFrom.toString();
							}
							
							XMPPError xmpError = stanza.getError();
							if(xmpError != null) {
								processErrorMessage(from, body, xmpError);
							}
							else {
								processMessage(from, body);
							}
						}
						else {
							logger.debug("processStanza(" + stanza + ") Message.getBody() -> " + body);
						}
					}
					else {
						if(stanza != null) {
							XMPPError xmpError = stanza.getError();
							if(xmpError != null) {
								logger.debug("processStanza(" + stanza + ") stanza.getError() -> " + xmpError);
							}
						}
						logger.debug("processStanza(" + stanza + ") stanza is NOT instanceof Message");
					}
				}
			}, StanzaTypeFilter.MESSAGE);
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.MessagingClient", ex);
			throw ex;
		}
	}
	
	public
	String getHost()
	{
		return host;
	}
	
	public
	String getUser()
	{
		return user;
	}
	
	public
	String getDomain()
	{
		return domain;
	}
	
	public
	boolean connect()
		throws Exception
	{
		logger.debug("XMessagingClient.connect()...");
		
		if(connection == null) {
			logger.debug("XMessagingClient.connect() -> Exception(\"Client not initialized\")");
			throw new Exception("Client not initialized");
		}
		
		try {
			logger.debug("connection.connect()...");
			connection.connect();
			
			logger.debug("connection.login()...");
			connection.login();
			
			logger.debug("roster = Roster.getInstanceFor(connection)...");
			roster = Roster.getInstanceFor(connection);
			
			logger.debug("chatManager = ChatManager.getInstanceFor(connection)...");
			chatManager = ChatManager.getInstanceFor(connection);
			
			logger.debug("accountManager = AccountManager.getInstance(connection)...");
			accountManager = AccountManager.getInstance(connection);
			accountManager.sensitiveOperationOverInsecureConnection(true);
			
			logger.debug("connection.isConnected()...");
			boolean result = connection.isConnected();
			
			logger.debug("XMessagingClient.connect() -> " + result);
			return result;
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.connect", ex);
			throw ex;
		}
	}
	
	public
	boolean isConnected()
		throws Exception
	{
		// Poiche' questo metodo potrebbe essere richiamato frequentemente
		// non si traccia sui log l'esito positivo del controllo
		if(connection == null) {
			logger.debug("XMessagingClient.isConnected() -> false (connection = null)");
			return false;
		}
		boolean result = false;
		try {
			result = connection.isConnected();
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.isConnected", ex);
			throw ex;
		}
		if(!result) {
			logger.debug("XMessagingClient.isConnected() -> " + result);
		}
		return result;
	}
	
	public 
	boolean createAccount(String username, String password) 
		throws Exception 
	{
		logger.debug("XMessagingClient.createAccount(" + username + "," + password + ")...");
		if(WUtil.isBlank(username)) {
			logger.debug("XMessagingClient.createAccount(" + username + "," + password + ") -> false");
			return false;
		}
		if(WUtil.isBlank(password)) {
			logger.debug("XMessagingClient.createAccount(" + username + "," + password + ") -> false");
			return false;
		}
		if(roster == null || accountManager == null) {
			logger.debug("XMessagingClient.createAccount(" + username + "," + password + ") -> false (roster=" + roster + ",accountManager=" + accountManager + ")");
			return false;
		}
		username = username.trim().toLowerCase();
		password = password.trim();
		
		try {
			logger.debug("roster.reload()...");
			roster.reload();
		}
		catch(Exception ex) {
			logger.error("Eccezione during roster.reload", ex);
			throw ex;
		}
		
		try {
			logger.debug("Localpart lpUser = Localpart.from(" + username + ")...");
			Localpart lpUser = Localpart.from(username);
			
			logger.debug("if (!roster.contains(JidCreate.bareFrom(lpUser, connection.getServiceName())))...");
			if (!roster.contains(JidCreate.bareFrom(lpUser, connection.getServiceName()))) {
				
				logger.debug("accountManager.createAccount(" + lpUser + "," + password + ")...");
				accountManager.createAccount(lpUser, password);
				
				logger.debug("XMessagingClient.createAccount(" + username + "," + password + ") -> true");
				return true;
			}
			else {
				logger.debug("roster.contains(JidCreate.bareFrom(lpUser, connection.getServiceName())) -> true");
			}
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.createAccount(" + username + "," + password + ")", ex);
			throw ex;
		}
		logger.debug("XMessagingClient.createAccount(" + username + "," + password + ") -> false");
		return false;
	}
	
	public
	List<String> listAccounts()
		throws Exception
	{
		logger.debug("XMessagingClient.listAccounts()...");
		
		if(roster == null) {
			logger.debug("XMessagingClient.listAccounts() -> Exception(\"Roster not available\")");
			throw new Exception("Roster not available");
		}
		
		try {
			logger.debug("roster.reload()...");
			roster.reload();
		}
		catch(Exception ex) {
			logger.error("Eccezione during roster.reload", ex);
			throw ex;
		}
		
		List<String> listResult = null;
		try {
			logger.debug("roster.getEntries()...");
			Set<RosterEntry> setOfRosterEntry = roster.getEntries();
			if(setOfRosterEntry != null) {
				listResult = new ArrayList<String>(setOfRosterEntry.size());
				
				Iterator<RosterEntry> iterator = setOfRosterEntry.iterator();
				while(iterator.hasNext()) {
					RosterEntry rosterEntry = iterator.next();
					
					BareJid bareJid = rosterEntry.getJid();
					if(bareJid == null) continue;
					
					Localpart localpart = bareJid.getLocalpartOrNull();
					if(localpart == null) continue;
					
					listResult.add(localpart.toString());
				}
			}
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.listAccounts()", ex);
			throw ex;
		}
		if(listResult == null) listResult = new ArrayList<String>(0);
		logger.debug("XMessagingClient.listAccounts() -> " + WUtil.size(listResult) + " items");
		return listResult;
	}
	
	public
	XMessage sendMessage(XMessage xmessage)
		throws Exception
	{
		logger.debug("XMessagingClient.sendMessage(" + xmessage + ")...");
		
		// Controlli bloccanti
		if(chatManager == null) {
			logger.debug("XMessagingClient.sendMessage -> Exception(\"Client not connected\")");
			throw new Exception("Client not connected");
		}
		if(xmessage == null) {
			logger.debug("XMessagingClient.sendMessage -> Exception(\"Invalid message: object is null.\")");
			throw new Exception("Invalid message: object is null.");
		}
		if(WUtil.isBlank(xmessage.getTo())) {
			logger.debug("XMessagingClient.sendMessage -> Exception(\"Invalid message: to is empty.\")");
			throw new Exception("Invalid message: to is empty.");
		}
		// Eventuale completamento attributi
		if(WUtil.isBlank(xmessage.getFrom())) {
			xmessage.setFrom(user);
		}
		if(WUtil.isBlank(xmessage.getCreation())) {
			xmessage.setCreation(new Date());
		}
		if(WUtil.isBlank(xmessage.getUpdate())) {
			xmessage.setUpdate(new Date());
		}
		if(WUtil.isBlank(xmessage.getChannelType())) {
			xmessage.setChannelType(XMessage.DEFAULT_CHANNEL_TYPE);
		}
		if(WUtil.isBlank(xmessage.getType())) {
			xmessage.setType(XMessage.DEFAULT_OUTBOUND_TYPE);
		}
		if(WUtil.isBlank(xmessage.getThread())) {
			xmessage.setThread(UUID.randomUUID().toString());
		}
		String to = xmessage.getTo();
		if(to != null) to = to.trim().toLowerCase();
		
		try {
			EntityBareJid jidFrom = JidCreate.entityBareFrom(Localpart.from(user), JidCreate.domainBareFrom(domain));
			EntityBareJid jidTo   = JidCreate.entityBareFrom(Localpart.from(to),   JidCreate.domainBareFrom(domain));
			
			Message message = new Message();
			message.setFrom(jidFrom);
			message.setTo(jidTo);
			message.setBody(message.toString());
			
			logger.debug("chatManager.chatWith(" + jidTo + ")...");
			Chat chat = chatManager.chatWith(jidTo);
			
			logger.debug("chatManager.send...");
			chat.send(message);
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.sendMessage", ex);
			try {
				logger.debug("XMessagingManager.insert(message=" + xmessage.getThread() + ")...");
				xmessage.setSent(Boolean.FALSE);
				xmessage.setError(ex.toString());
				xmessage = XMessagingManager.insert(xmessage);
			}
			catch(Exception exi) {
				logger.error("Eccezione in XMessagingClient.sendMessage during insert", exi);
			}
			throw ex;
		}
		try {
			logger.debug("XMessagingManager.insert(message=" + xmessage.getThread() + ")...");
			xmessage.setSent(Boolean.TRUE);
			xmessage = XMessagingManager.insert(xmessage);
		}
		catch(Exception ex) {
			logger.error("Eccezione in XMessagingClient.sendMessage during insert " + xmessage.getThread(), ex);
			// Non si propaga l'eccezione: e' importante che sia stato inviato il messaggio
		}
		logger.debug("XMessagingClient.sendMessage -> " + xmessage.getThread());
		return xmessage;
	}
	
	public
	void disconnect()
	{
		logger.debug("XMessagingClient.disconnect()...");
		if(connection != null) {
			logger.debug("connection.disconnect()...");
			connection.disconnect();
		}
		else {
			logger.debug("connection is null...");
		}
	}
	
	protected
	void processMessage(String from, String body)
	{
		logger.debug("XMessagingClient.processMessage(" + from + "," + body + ")...");
		
		XMessage message = new XMessage(body);
		message.setFrom(from);
		message.setTo(user);
		message.setUpdate(new Date());
		
		try {
			XMessagingManager.insert(message);
		}
		catch(Exception ex) {
			logger.error("processMessage(" + from + "," + body + "):", ex);
		}
		
		try {
			XMessagingManager.reportResponse(message);
		}
		catch(Exception ex) {
			logger.error("processMessage(" + from + "," + body + "):", ex);
		}
	}
	
	protected
	void processErrorMessage(String from, String body, XMPPError xmppError)
	{
		logger.debug("XMessagingClient.processErrorMessage(" + from + "," + body + "," + xmppError + ")...");
		
		XMessage message = new XMessage(body);
		message.setFrom(from);
		message.setTo(user);
		message.setUpdate(new Date());
		
		try {
			XMessagingManager.reportError(message, xmppError);
		}
		catch(Exception ex) {
			logger.error("processErrorMessage(" + from + "," + body + "):", ex);
		}
	}
}
