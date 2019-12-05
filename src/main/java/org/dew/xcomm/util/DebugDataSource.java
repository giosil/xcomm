package org.dew.xcomm.util;

import java.util.*;
import java.io.*;
import java.sql.*;

public
class DebugDataSource
{
	public static Properties config = new Properties();
	
	static {
		InputStream oIn = null;
		try {
			oIn = Thread.currentThread().
			getContextClassLoader().
			getResource("jdbc_debug.cfg").openStream();
			config.load(oIn);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		finally {
			try { oIn.close(); } catch(Exception oEx) {}
		}
		
		System.out.println("File jdbc_debug.cfg loaded.");
	}
	
	public static
	Connection getConnection(String sName)
		throws Exception
	{
		String sDriver = config.getProperty(sName + ".driver");
		Class.forName(sDriver);
		
		String sURL = config.getProperty(sName + ".url");
		String sUser = config.getProperty(sName + ".user");
		String sPassword = config.getProperty(sName + ".password");
		
		Connection conn = DriverManager.getConnection(sURL, sUser, sPassword);
		conn.setAutoCommit(false);
		
		return conn;
	}
}
