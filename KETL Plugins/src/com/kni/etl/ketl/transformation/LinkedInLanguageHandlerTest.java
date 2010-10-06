package com.kni.etl.ketl.transformation;

import junit.framework.TestCase;

import org.w3c.dom.Document;

import com.kni.etl.util.XMLHelper;

public class LinkedInLanguageHandlerTest extends TestCase {

	public void testGetNodes() throws Exception {
		Document doc = XMLHelper
				.readXMLFromFile("C:\\development\\LiveCode\\workspace\\KETL Tests\\xml\\LinkedIn\\data.xml");
		LinkedInLanguageHandler handler = new LinkedInLanguageHandler();
		for (Document o : handler.getNodes(doc)) {
			System.out.println(XMLHelper.outputXML(o,true));
			System.out.println("..................................");
		}
	}

}
