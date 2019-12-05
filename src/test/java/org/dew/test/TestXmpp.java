package org.dew.test;

import org.jivesoftware.smack.AbstractXMPPConnection;
import org.jivesoftware.smack.ConnectionListener;
import org.jivesoftware.smack.StanzaListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.ConnectionConfiguration.SecurityMode;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.chat2.Chat;
import org.jivesoftware.smack.chat2.ChatManager;
import org.jivesoftware.smack.filter.StanzaTypeFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Stanza;
import org.jivesoftware.smack.packet.XMPPError;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;

import org.jxmpp.jid.Jid;
import org.jxmpp.jid.impl.JidCreate;
import org.jxmpp.jid.parts.Localpart;

public 
class TestXmpp 
{
  // https://[ejabberd_host]/admin/server/xcomm.dew.org/
  // user: xcomm@xcomm.dew.org
  // pass: XXXX
  
  // user2: a0199997
  // pass2: XXXX
  
  public static void main(String[] args) {
    try {
      System.out.println("TestXmpp...");
      
      System.out.println("XMPPTCPConnectionConfiguration.builder()...");
      XMPPTCPConnectionConfiguration.Builder configBuilder = XMPPTCPConnectionConfiguration.builder();
      configBuilder.setSecurityMode(SecurityMode.required);
      configBuilder.setUsernameAndPassword("xcomm", "FbfxGOZV708pTc");
      configBuilder.setHost("ejabberd_host");
      configBuilder.setXmppDomain(JidCreate.domainBareFrom("xcomm.dew.org"));
      
      System.out.println("new XMPPTCPConnection...");
      AbstractXMPPConnection connection = new XMPPTCPConnection(configBuilder.build());
      
      System.out.println("connection.connect()...");
      connection.connect();
      
      System.out.println("connection.addConnectionListener...");
      connection.addConnectionListener(new ConnectionListener() {
        public void reconnectionSuccessful() {
          System.out.println("reconnectionSuccessful");
        }
        public void reconnectionFailed(Exception exception) {
          System.out.println("reconnectionFailed(" + exception + ")");
        }
        public void reconnectingIn(int seconds) {
          System.out.println("reconnectingIn(" + seconds + ")");
        }
        public void connectionClosedOnError(Exception exception) {
          System.out.println("connectionClosedOnError(" + exception + ")");
        }
        public void connectionClosed() {
          System.out.println("connectionClosed()");
        }
        public void connected(XMPPConnection conn) {
          System.out.println("connected(" + conn + ")");
        }
        public void authenticated(XMPPConnection conn, boolean resumed) {
          System.out.println("authenticated(" + conn + "," + resumed + ")");
        }
      });
      
      System.out.println("connection.addAsyncStanzaListener...");
      connection.addAsyncStanzaListener(new StanzaListener() {
        public void processStanza(Stanza stanza) throws NotConnectedException, InterruptedException {
          System.out.println("processStanza(" + stanza + ")...");
          
          if(stanza != null) {
            XMPPError xmpError = stanza.getError();
            if(xmpError != null) {
              System.out.println("processStanza(" + stanza + ") stanza.getError() -> " + xmpError);
            }
          }
          
          if (stanza instanceof Message){
            Message message = Message.class.cast(stanza);
            String sBody = message.getBody();
            if(sBody != null) {
              Jid from = message.getFrom();
              Localpart localpartFrom = from.getLocalpartOrNull();
              System.out.println("[" + localpartFrom + "]: " + sBody);
            }
          }
        }
      }, StanzaTypeFilter.MESSAGE);
      
      System.out.println("connection.login...");
      connection.login();
      
      System.out.println("ChatManager.getInstanceFor...");
      ChatManager chatManager = ChatManager.getInstanceFor(connection);
      
      System.out.println("Prepare message...");
      Message message = new Message();
      message.setBody("Test");
      
      System.out.println("chatManager.chatWith...");
      Chat chat = chatManager.chatWith(JidCreate.entityBareFrom(Localpart.from("a0199997"), JidCreate.domainBareFrom("xcomm.dew.org")));
      
      System.out.println("chat.send(message)...");
      chat.send(message);
      
      System.out.println("Thread.sleep...");
      Thread.sleep(10 * 1000);
      
      System.out.println("connection.disconnect()...");
      connection.disconnect();
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    System.out.println("End.");
  }

}
