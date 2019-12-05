package org.dew.xcomm.util;

import java.util.*;

import org.apache.log4j.*;

import org.dew.xcomm.messaging.XMessagingClient;

public
class CheckTimerTask extends TimerTask
{
  protected transient Logger logger = Logger.getLogger(getClass());
  
  protected boolean firstTime = true;
  
  public
  void run()
  {
    try {
      XMessagingClient client = XMessagingClient.getInstance();
      
      if(client.isConnected()) {
        if(firstTime) {
          // Si evita di intasare il log in caso di esito OK.
          logger.debug("client.isConnected() -> true");
        }
      }
      else {
        logger.warn("client.isConnected() -> false");
        logger.warn("client.connect()...");
        client.connect();
      }
      
      firstTime = false;
    }
    catch(Exception ex) {
      logger.error("Eccezione in CheckConnectionsTimerTask.run", ex);
    }
  }
}
