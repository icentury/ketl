/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jun 5, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.util;

import java.io.FileReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.EngineConstants;
import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.stringtools.StringMatcher;

/**
 * @author bsullivan To change this generated comment go to Window>Preferences>Java>Code Generation>Code and Comments
 */
public class XMLHelper {

    public static final String NAME_TAG = "NAME";
    public static final String PARAMETER_LIST_TAG = "PARAMETER_LIST";
    public static final String PARAMETER_OVERRIDE_ATTRIB = "OVERRIDE";
    public static final String PARAMETER_TAG = "PARAMETER";

    public static synchronized Document readXMLFromFile(String pFileName) throws Exception {
        Document xmlDocument = null;

        // Build a DOM out of the XML string...
        // Read XML config file
        StringBuffer sb = new StringBuffer();
        FileReader inputFileReader = null;
        try {
            inputFileReader = new FileReader(pFileName);
            int c;

            while ((c = inputFileReader.read()) != -1) {
                sb.append((char) c);
            }

            // turn file into readable nodes
            DocumentBuilder builder = null;

            // Build a DOM out of the XML string...

            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            builder = dmf.newDocumentBuilder();
            xmlDocument = builder.parse(new InputSource(new StringReader(sb.toString())));

        } catch (org.xml.sax.SAXException e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Parsing XML document("
                    + pFileName + "), " + e.toString());

            return null;
        } catch (Exception e) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Problem reading XML file ("
                    + pFileName + ")" + e.toString());

            return null;
        } finally {
            if (inputFileReader != null)
                inputFileReader.close();
        }

        return xmlDocument;
    }

    public static void listParameterLists(Node node, HashMap list) {
        if (node != null) {
            NamedNodeMap nm = node.getAttributes();

            if (nm != null) {
                Node x = nm.getNamedItem(PARAMETER_LIST_TAG);

                if (x != null) {
                    list.put(x.getNodeValue(), x.getNodeValue());
                }
            }

            NodeList nl = node.getChildNodes();

            for (int i = 0; i < nl.getLength(); i++) {
                listParameterLists(nl.item(i), list);
            }
        }
    }

    private static String dumpattributes(NamedNodeMap attribs) {
        if (attribs == null) {
            return "";
        }

        StringBuffer sb = new StringBuffer("");

        for (int i = 0; i < attribs.getLength(); i++) {
            Node n = attribs.item(i);
            sb.append(" " + n.getNodeName() + "=\"" + escapeXML(n.getNodeValue()) + "\"");
        }

        return sb.toString();
    }

    private static String escapeXML(String pXML) {
        String str = pXML.replaceAll("&", "&amp;");
        str = str.replaceAll("\"", "&quot;");
        str = str.replaceAll("<", "&lt;");
        str = str.replaceAll(">", "&gt;");
        str = str.replaceAll("’", "&apos;");

        return str;
    }

    private static String outputXML2(Node node) {
        String result = null;
        Document document = node.getOwnerDocument();
        if (document != null) {
            StringWriter strWtr = new StringWriter();
            StreamResult strResult = new StreamResult(strWtr);
            TransformerFactory tfac = TransformerFactory.newInstance();
            try {
                Transformer t = tfac.newTransformer();
                t.setOutputProperty(OutputKeys.INDENT, "yes");
                t.setOutputProperty(OutputKeys.METHOD, "xml"); // xml, html, text
                t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                t.transform(new DOMSource(document.getDocumentElement()), strResult);
            } catch (Exception e) {
                System.err.println("XML.toString(Document): " + e);
            }
            result = strResult.getWriter().toString();
        }

        return result;

    }

    public static String outputXML(Node node) {
        return outputXML(node, false);
    }

    public static String outputXML(Node node, boolean bTopLevel) {
        if (node == null) {
            return "";
        }

        if (bTopLevel)
            return outputXML2(node);

        StringBuffer sb = new StringBuffer("");
        sb.append("<" + node.getNodeName() + dumpattributes(node.getAttributes()));

        NodeList nl = node.getChildNodes();
        boolean endAdded = false;

        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);

            if (n.getNodeType() == Node.ELEMENT_NODE) {
                if (endAdded == false) {
                    sb.append(">");
                    endAdded = true;
                }

                sb.append(outputXML(nl.item(i)));
            }
            else if (n.getNodeType() == Node.TEXT_NODE) {
                Node nc = node.getFirstChild();
                String val = null;

                if ((nc != null) && (nc.getNodeType() == Node.TEXT_NODE)) {
                    val = nc.getNodeValue();
                }

                if ((val != null) && (val.length() > 0)) {
                    if (endAdded == false) {
                        sb.append(">");
                        endAdded = true;
                    }

                    sb.append(escapeXML(val));
                }
            }
        }

        if (endAdded) {
            sb.append("</" + node.getNodeName() + ">");
        }
        else {
            sb.append("/>");
        }

        return sb.toString();

    }

    public static int getAttributeAsInt(NamedNodeMap nmAttrs, String attributeName, int defaultValue) {
        try {
            return (Integer.parseInt(nmAttrs.getNamedItem(attributeName).getNodeValue()));
        } catch (NumberFormatException e1) {
            ResourcePool.LogMessage(Thread.currentThread().getName(),
                    ResourcePool.WARNING_MESSAGE,"Invalid number specified as attribute value"
                            + XMLHelper.outputXML(nmAttrs.getNamedItem(attributeName)));

            return defaultValue;
        } catch (DOMException e1) {
            return defaultValue;
        } catch (NullPointerException e1) {
            return defaultValue;
        }
    }

    public static boolean getAttributeAsBoolean(NamedNodeMap nmAttrs, String attributeName, boolean defaultValue) {
        try {
            String res = nmAttrs.getNamedItem(attributeName).getNodeValue();

            if (res.toUpperCase().compareTo("TRUE") == 0) {
                return true;
            }
            else if (res.toUpperCase().compareTo("FALSE") == 0) {
                return false;
            }
            else {
                return defaultValue;
            }
        } catch (NumberFormatException e1) {
            ResourcePool.LogMessage(Thread.currentThread().getName(),
                    ResourcePool.WARNING_MESSAGE,"Invalid number specified as attribute value"
                            + XMLHelper.outputXML(nmAttrs.getNamedItem(attributeName)));

            return defaultValue;
        } catch (DOMException e1) {
            return defaultValue;
        } catch (NullPointerException e1) {
            return defaultValue;
        }
    }

    public static String getAttributeAsString(NamedNodeMap nmAttrs, String attributeName, String defaultValue) {
        try {
            if(nmAttrs == null || nmAttrs.getNamedItem(attributeName) == null) return defaultValue;
            
            String str = nmAttrs.getNamedItem(attributeName).getNodeValue();

            return (decodeHex(str));
        } catch (NullPointerException e1) {
            return defaultValue;
        } catch (DOMException e1) {
            return defaultValue;
        }
    }

    // \n Newline (Linefeed)
    // \t Tab
    // \v Vertical tab
    // \b Backspace
    // \r Return
    // \f Form feed
    // \a Alert (Bell)
    // \### Each # is an octal digit.
    // \###### Each # is an octal digit - Unicode character code
    // \ x## Each # is a hexadecimal digit
    // \ u#### Where #### are four hexadecimal digits that specify a Unicode character
    // \\ Backslash (\)
    // \' Single quote (')
    // \" Double quote (")
    public static String decodeHex(String pString) {
        String str = pString;

        if (str == null) {
            return null;
        }

        int pos;
        int i = 0;

        do {
            pos = str.indexOf("#", i);

            if (pos != -1) {
                String strHex;

                if ((pos + 3) < str.length()) {
                    strHex = str.substring(pos, pos + 3);
                }
                else {
                    strHex = str.substring(pos);
                }

                try {
                    Integer x = Integer.decode(strHex);
                    char ch = (char) x.intValue();
                    str = str.replaceAll(strHex, String.valueOf(ch));
                } catch (NumberFormatException e) {
                    i++;
                }
            }
        } while (pos != -1);

        return str;
    }

    public static String getNodeValueAsString(Node node, String strDefaultValue) {
        if (node == null) {
            return strDefaultValue;
        }

        return node.getNodeValue();
    }

    public static String getChildNodeValueAsString(Node xmlNode, String strTagName, String strElementAttribute,
            String strElementValue, String strDefaultValue) {
        Node childNode = getElementByName(xmlNode, strTagName, strElementAttribute, strElementValue);

        if (childNode != null) {
            return getNodeValueAsString(childNode.getFirstChild(), strDefaultValue);
        }

        return strDefaultValue;
    }

    // Just a wrapper to return the first node we find (common case)
    public static Node getElementByName(Node xmlNode, String strTagName, String strElementAttribute,
            String strElementValue) {
        Node[] nodes = getElementsByName(xmlNode, strTagName, strElementAttribute, strElementValue);

        if ((nodes == null) || (nodes.length == 0)) {
            return null;
        }

        return nodes[0];
    }

    // Returns the first child Node found in the given node's DOM of a particular tag type having a particular attribute
    // with the given value, or null if not found.
    public static Node[] getElementsByName(Node xmlNode, String strTagName, String strElementAttribute,
            String strElementValue) {
        if (xmlNode == null) {
            return null;
        }

        NodeList nl = xmlNode.getChildNodes();

        return getElementsByName(nl, strTagName, strElementAttribute, strElementValue);
    }

    // Returns the first Node found in the given node's DOM of a particular tag type having a particular attribute with
    // the given value, or null if not found.
    // Note that this searches the entire DOM of the node, not just it's subtree.
    public static Node findElementByName(Node xmlNode, String strTagName, String strElementAttribute,
            String strElementValue) {
        if (xmlNode == null) {
            return null;
        }

        NodeList nl = null;

        if (xmlNode instanceof Document) {
            nl = ((Document) xmlNode).getElementsByTagName(strTagName);
        }
        else {
            nl = xmlNode.getOwnerDocument().getElementsByTagName(strTagName);
        }

        Node[] matchingNodes = getElementsByName(nl, strTagName, strElementAttribute, strElementValue);

        if ((matchingNodes == null) || (matchingNodes.length == 0)) {
            return null;
        }

        return matchingNodes[0];
    }

    // Returns the first Node found in the given node's DOM of a particular tag type having a particular attribute with
    // the given value, or null if not found.
    // Note that this searches the entire DOM of the node, not just it's subtree.
    public static Node[] findElementsByName(Node xmlNode, String strTagName, String strElementAttribute,
            String strElementValue) {
        if (xmlNode == null) {
            return null;
        }

        NodeList nl = null;

        if (xmlNode instanceof Document) {
            nl = ((Document) xmlNode).getElementsByTagName(strTagName);
        }
        else {
            nl = xmlNode.getOwnerDocument().getElementsByTagName(strTagName);
        }

        Node[] matchingNodes = getElementsByName(nl, strTagName, strElementAttribute, strElementValue);

        if ((matchingNodes == null) || (matchingNodes.length == 0)) {
            return null;
        }

        return matchingNodes;
    }

    // Returns the Nodes found in the given node list of a particular tag type having a particular attribute with the
    // given value, or null if not found.
    static Node[] getElementsByName(NodeList nl, String strTagName, String strElementAttribute, String strElementValue) {
        NamedNodeMap nmAttrs;
        Node node = null;
        StringMatcher sm = null;
        if (strElementValue != null)
            sm = new StringMatcher(strElementValue);
        ArrayList alMatchingNodes = null;

        if ((nl == null) || (nl.getLength() == 0)) {
            // No tags of that name specified...
            return null;
        }

        int len = nl.getLength();

        for (int i = 0; i < len; i++) {
            Node currentNode = nl.item(i);
            // Check to make sure that the element has the correct tag name...
            if (strTagName.equals(currentNode.getNodeName()) == false) {
                continue;
            }

            if (alMatchingNodes == null)
                alMatchingNodes = new ArrayList(1); // default is 10, which is wasteful 99.9% of the time

            // If we haven't specified an attribute name, then match all...
            if (strElementAttribute == null) {
                // Found a matching element - add it to the list...
                alMatchingNodes.add(currentNode);

                continue;
            }

            // See if we can match the name of the element...
            if ((nmAttrs = currentNode.getAttributes()) == null) {
                continue;
            }

            node = nmAttrs.getNamedItem(strElementAttribute);

            if (strElementAttribute.equals("*")) {
                alMatchingNodes.add(currentNode);
            }
            else if (node != null) {
                if (sm == null || sm.match(node.getNodeValue())) {
                    // Found a matching element - add it to the list...
                    alMatchingNodes.add(nl.item(i));
                }
            }
        }

        if (alMatchingNodes == null)
            return NONODES;
        
        Node[] anResult = new Node[alMatchingNodes.size()];

        alMatchingNodes.toArray(anResult);

        return anResult;
    }

    public static final String getTextContent(Node node){
        return  node==null||node.getFirstChild()==null?null:node.getTextContent();
        

    }
    private static final Node[] NONODES = new Node[0];

    public static String getParameterValueAsString(Node xmlNode, String strParameterListName, String strParameterName,
            String strDefaultValue) {
        return XMLHelper.getParameterValueAsString(xmlNode, strParameterListName, strParameterName, strDefaultValue,
                false);
    }

    // If you set xmlNode = null, then it will only look in the metadata
    // Returns: strDefaultValue if we can't find the value for ANY REASON.
    public static String getParameterValueAsString(Node xmlNode, String strParameterListName, String strParameterName,
            String strDefaultValue, boolean recurse) {
        Node node;

        // NamedNodeMap nmAttrs;
        // Need at least a parameter list name and parameter name...
        if ((strParameterListName == null) || (strParameterName == null)) {
            return strDefaultValue;
        }

        if (xmlNode != null) {
            // check for parameter overrides, if the parameter name exists in the
            // <PARAMETER_OVERRIDE> tag
            // then use that else look in XML then db.
            if ((node = XMLHelper.findElementByName(xmlNode, PARAMETER_LIST_TAG, PARAMETER_OVERRIDE_ATTRIB, "TRUE")) != null) {
                Node[] overrideNodes = getElementsByName(node, PARAMETER_TAG, PARAMETER_LIST_TAG, strParameterListName);
                String res = null;

                if (overrideNodes != null) {
                    for (int i = 0; i < overrideNodes.length; i++) {
                        String paramName = getAttributeAsString(overrideNodes[i].getAttributes(), NAME_TAG, null);

                        if ((paramName != null) && paramName.equals(strParameterName)) {
                            if (overrideNodes[i].getNodeType() == Node.ELEMENT_NODE) {
                                res = overrideNodes[i].getFirstChild().getNodeValue();
                            }
                            else {
                                res = overrideNodes[i].getNodeValue();
                            }

                            if (res != null) {
                                continue;
                            }
                        }
                    }
                }

                if (res == null) {
                    overrideNodes = getElementsByName(node, PARAMETER_TAG, NAME_TAG, strParameterName);

                    if (overrideNodes != null) {
                        for (int i = 0; i < overrideNodes.length; i++) {
                            String hasName = getAttributeAsString(overrideNodes[i].getAttributes(), PARAMETER_LIST_TAG,
                                    null);

                            if (hasName == null) {
                                if (overrideNodes[i].getNodeType() == Node.ELEMENT_NODE) {
                                    res = overrideNodes[i].getFirstChild().getNodeValue();
                                }
                                else {
                                    res = overrideNodes[i].getNodeValue();
                                }

                                if (res != null) {
                                    continue;
                                }
                            }
                        }
                    }
                }

                if (res != null) {
                    return res;
                }
            }

            // Allow local file declarations to shadow the metadata ones.
            // First dig from the root to find the connection information for the parameter list we're looking for...
            // If xmlNode is null, then getElement will return null...
            if ((node = XMLHelper.findElementByName(xmlNode, PARAMETER_LIST_TAG, NAME_TAG, strParameterListName)) != null) {
                Node paramNode = getElementByName(node, PARAMETER_TAG, NAME_TAG, strParameterName);

                if (paramNode == null)
                    return strDefaultValue;

                String val = XMLHelper.getTextContent(paramNode);
                String[] requiredParameters = EngineConstants.getParametersFromText(val);
                if (recurse && requiredParameters != null) {
                    String subParam = XMLHelper.getAttributeAsString(paramNode.getAttributes(), PARAMETER_LIST_TAG,
                            null);
                    for (int i = 0; i < requiredParameters.length; i++) {
                        String str = XMLHelper.getParameterValueAsString(xmlNode, strParameterListName,
                                requiredParameters[i], null);

                        if (str == null)
                            str = XMLHelper.getParameterValueAsString(xmlNode, subParam, requiredParameters[i], null);
                        if (str == null) {
                            Node n = paramNode.getParentNode();
                            if (n != null && n.getNodeName().equals(PARAMETER_LIST_TAG)) {
                                str = XMLHelper.getAttributeAsString(n.getAttributes(), NAME_TAG, null);
                                str = XMLHelper.getParameterValueAsString(xmlNode, str, requiredParameters[i], null);
                            }
                        }
                        if (str != null)
                            val = EngineConstants.replaceParameter(val, requiredParameters[i], str);
                    }

                }
                return val;
            }
        }

        // If we didn't find it in the XML, then call the metadata to look for it...
        if (ResourcePool.getMetadata() == null) {
            return strDefaultValue;
        }

        Object[][] parameterList = ResourcePool.getMetadata().getParameterList(strParameterListName);

        // If we can't find the parameter list there either, then we're in trouble...just return the default...
        if (parameterList == null) {
            return strDefaultValue;
        }

        for (int i = 0; i < parameterList.length; i++) {
            // If we found the parameter we're looking for, return it's value...
            if (strParameterName.compareTo((String) parameterList[i][Metadata.PARAMETER_NAME]) == 0) {

                String val = (String) parameterList[i][Metadata.PARAMETER_VALUE];

                if (val == null)
                    return null;

                String[] requiredParameters = EngineConstants.getParametersFromText(val);

                if (requiredParameters != null) {
                    for (int x = 0; x < requiredParameters.length; x++) {
                        String str = XMLHelper.getParameterValueAsString(xmlNode, strParameterListName,
                                requiredParameters[x], null);

                        if (str == null) {
                            str = XMLHelper.getParameterValueAsString(xmlNode,
                                    (String) parameterList[i][Metadata.SUB_PARAMETER_LIST_NAME], requiredParameters[x],
                                    null);
                        }
                        if (str != null)
                            val = EngineConstants.replaceParameter(val, requiredParameters[x], str);
                    }
                }
                return val;
            }
        }

        // If we've gotten this far, then we didn't find what we were looking for...
        return strDefaultValue;
    }

    public static String[] getDistinctParameterNames(Node xmlNode, String strParameterListName) {
        Node node;

        // NamedNodeMap nmAttrs;
        // temp holding area
        HashSet tmpList = new HashSet();

        // Need at least a parameter list name and parameter name...
        if (strParameterListName == null) {
            return null;
        }

        // There are two ways to specify datastore information: explicitly in the file or in the metadata.
        // Allow local file declarations to shadow the metadata ones.
        // First dig from the root to find the connection information for the parameter list we're looking for...
        // If xmlNode is null, then getElement will return null...
        if (xmlNode != null
                && (node = XMLHelper.findElementByName(xmlNode, PARAMETER_LIST_TAG, NAME_TAG, strParameterListName)) != null) {
            NodeList nl = node.getChildNodes();

            // get parameter list name from array of nodes which had parameter NAME equal to value
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);

                if (n.getNodeType() == Node.ELEMENT_NODE) {
                    String parameterName = XMLHelper.getAttributeAsString(n.getAttributes(), NAME_TAG, null);
                    tmpList.add(parameterName);
                }
            }
        }
        else // If we didn't find it in the XML, then call the metadata to look for it...
        {
            if (ResourcePool.getMetadata() == null) {
                return null;
            }

            Object[][] parameterList = ResourcePool.getMetadata().getParameterList(strParameterListName);

            // If we can't find the parameter list there either, then we're in trouble...
            if (parameterList == null) {
                return null;
            }

            for (int i = 0; i < parameterList.length; i++) {
                tmpList.add(parameterList[i][Metadata.PARAMETER_NAME]);
            }
        }

        // If none found, return null...
        if (tmpList.size() == 0) {
            return null;
        }

        // copy elements from arraylist into array of correct size
        String[] aResult = new String[tmpList.size()];

        tmpList.toArray(aResult);

        return aResult;
    }

    // If you set xmlNode = null, then it will only look in the metadata
    public static String[] getSubParameterListNames(Node xmlNode, String strParameterListName, String strParameterName) {
        Node node;

        // NamedNodeMap nmAttrs;
        // temp holding area
        ArrayList tmpList = new ArrayList();

        // Need at least a parameter list name and parameter name...
        if ((strParameterListName == null) || (strParameterName == null)) {
            return null;
        }

        // There are two ways to specify datastore information: explicitly in the file or in the metadata.
        // Allow local file declarations to shadow the metadata ones.
        // First dig from the root to find the connection information for the parameter list we're looking for...
        // If xmlNode is null, then getElement will return null...
        if (xmlNode != null
                && ((node = XMLHelper.findElementByName(xmlNode, PARAMETER_LIST_TAG, NAME_TAG, strParameterListName)) != null)) {
            Node[] aNodes = getElementsByName(node, PARAMETER_TAG, NAME_TAG, strParameterName);

            if ((aNodes == null) || (aNodes.length == 0)) {
                return null;
            }

            // get parameter list name from array of nodes which had parameter NAME equal to value
            for (int i = 0; i < aNodes.length; i++) {
                String value = XMLHelper.getAttributeAsString(aNodes[i].getAttributes(), PARAMETER_LIST_TAG, null);

                if (value != null) {
                    Node[] nodes = XMLHelper.findElementsByName(xmlNode, PARAMETER_LIST_TAG, NAME_TAG, value);

                    if (nodes != null) {
                        for (int x = 0; x < nodes.length; x++) {
                            tmpList.add(XMLHelper.getAttributeAsString(nodes[x].getAttributes(), NAME_TAG, null));
                        }
                    }
                    else if (ResourcePool.getMetadata() != null) // if not found in xml then look in db
                    {
                        String[] res = ResourcePool.getMetadata().getValidParameterListName(
                                value.replaceAll("\\*", "%"));

                        if (res != null) {
                            for (int x = 0; x < res.length; x++) {
                                tmpList.add(res[x]);
                            }
                        }
                    }
                }
            }
        }
        else // If we didn't find it in the XML, then call the metadata to look for it...
        {
            if (ResourcePool.getMetadata() == null) {
                return null;
            }

            Object[][] parameterList = ResourcePool.getMetadata().getParameterList(
                    strParameterListName.replaceAll("\\*", "%"));

            // If we can't find the parameter list there either, then we're in trouble...
            if (parameterList != null) {
                for (int i = 0; i < parameterList.length; i++) {
                    // If we found the parameter we're looking for then save it's name...
                    if (strParameterName.compareTo((String) parameterList[i][Metadata.PARAMETER_NAME]) == 0) {
                        // If it's null, then we'll just store the null for reference...
                        // if not null then find lists that matcgh pattern
                        if (parameterList[i][Metadata.SUB_PARAMETER_LIST_NAME] != null) {
                            String[] res = ResourcePool.getMetadata().getValidParameterListName(
                                    ((String) parameterList[i][Metadata.SUB_PARAMETER_LIST_NAME])
                                            .replaceAll("\\*", "%"));

                            if (res != null) {
                                for (int x = 0; x < res.length; x++) {
                                    tmpList.add(res[x]);
                                }
                            }
                        }

                        /*
                         * else { tmpList.add(null); }
                         */
                    }
                }
            }
        }

        // If none found, return null...
        if (tmpList.size() == 0) {
            return null;
        }

        // copy elements from arraylist into array of correct size
        String[] aResult = new String[tmpList.size()];

        tmpList.toArray(aResult);

        return aResult;
    }

    public void printTree(Node n, int depth) {
        NodeList nl = n.getChildNodes();

        if ((nl == null) || (nl.getLength() <= 1)) {
            for (int j = 0; j < depth; j++)
                System.out.print("\t");

            System.out.println("<" + n.getNodeName() + " />");
        }
        else {
            for (int j = 0; j < depth; j++)
                System.out.print("\t");

            System.out.println("<" + n.getNodeName() + ">");

            for (int i = 1; i < nl.getLength(); i++) {
                printTree(nl.item(i), depth + 1);
            }

            for (int j = 0; j < depth; j++)
                System.out.print("\t");

            System.out.println("</" + n.getNodeName() + ">");
        }
    }
}
