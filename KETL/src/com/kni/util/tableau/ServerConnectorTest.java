package com.kni.util.tableau;

import junit.framework.Assert;

import org.junit.Test;

import com.kni.util.tableau.ServerConnector.TableauResponse;

public class ServerConnectorTest {

	@SuppressWarnings("deprecation")
	@Test
	public void test() throws Exception {

		java.util.logging.Logger.getLogger("org.apache.http.wire").setLevel(java.util.logging.Level.FINEST);
		java.util.logging.Logger.getLogger("org.apache.http.headers").setLevel(java.util.logging.Level.FINEST);

		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
		System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
		System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "debug");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "debug");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.headers", "debug");
		
		ServerConnector client = new ServerConnector();
		client.authenticate(
				"https://online.tableausoftware.com/t/nextdoorcominc",
				"nick@nextdoor.com", "!nextdoor73");

		String project = "City Team";
		String name = "T40 Neighborhood Dashboard";
		
		TableauResponse tr = client.refreshExtract(project, ServerConnector.Type.workbook, name,true);
		Assert.assertTrue(tr.success());
	}

	

}
