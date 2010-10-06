package com.kni.etl.ketl.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ketl.transformation.XMLToFieldsTransformation.XMLNodeListCreator;
import com.kni.etl.util.XMLHelper;

public class LinkedInLanguageHandler implements XMLNodeListCreator {

	public List<Document> getNodes(Document doc) {

		List<Document> docs = new ArrayList();

		// determine each language
		// get by tag name where tag = le
		// will return a list of all le tags
		NodeList langs = doc.getElementsByTagName("le");

		// use set to dedupe list
		Set<String> languages = new HashSet();
		int size = langs.getLength();
		for (int i = 0; i < size; i++) {
			languages.add(XMLHelper.getAttributeAsString(langs.item(i).getAttributes(), "loc", null));
		}

		// duplicate document for each language
		// for language in set
		for (String lang : languages) {
			Document targetDoc = (Document) doc.cloneNode(true);
			NodeList languageNodes = targetDoc.getElementsByTagName("le");
			
			// add language to root node
			((Element)targetDoc.getFirstChild()).setAttribute("loc", lang);
			
			// some weird null error here, so have to preload list
			Set<Node> nodes = new HashSet();
			size = languageNodes.getLength();
			for (int i = 0; i < size; i++) {
				nodes.add(languageNodes.item(i));
			}

			
			for (Node currentNode : nodes) {
				// remove all other languages not in current set, maybe easier to rename tag
				String nodeLang = XMLHelper.getAttributeAsString(currentNode.getAttributes(), "loc", null);
				if (nodeLang.equals(lang) == false) {
					currentNode.getParentNode().removeChild(currentNode);
				}

			}
			// add document to output list
			docs.add(targetDoc);
			// end loop
		}
		// return results
		return docs;
	}

}
