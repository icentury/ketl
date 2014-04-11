package com.kni.util.tableau;

import org.junit.Test;

public class ServerConnectorTest {

	@Test
	public void test() throws Exception {

		ServerConnector client = new ServerConnector();
		client.authenticate(
				"https://online.tableausoftware.com/t/nextdoorcominc",
				"nick@nextdoor.com", "!nextdoor73");

		String project = "City Team";
		String name = "T40 Neighborhood Dashboard";
		
		client.refreshExtract(project, ServerConnector.Type.workbook, name,false);
		
	}

	

}
