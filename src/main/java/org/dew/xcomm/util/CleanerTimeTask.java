package org.dew.xcomm.util;

import java.util.Calendar;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import org.dew.xcomm.messaging.XMessagingManager;

public 
class CleanerTimeTask extends TimerTask
{
	protected transient Logger logger = Logger.getLogger(getClass());
	
	protected boolean firstTime = true;
	
	public
	void run()
	{
		try {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MONTH, -2);
			
			XMessagingManager.removeUpTo(cal.getTime());
		}
		catch(Exception ex) {
			logger.error("Eccezione in CleanerTimeTask.run", ex);
		}
	}
}
