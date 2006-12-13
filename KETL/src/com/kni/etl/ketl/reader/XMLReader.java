/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Mar 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.reader;

import java.io.File;
import java.lang.reflect.Constructor;
import java.text.Format;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.XMLHandler;
import com.kni.etl.util.XMLHelper;
import com.kni.util.FileTools;

/**
 * @author bsullivan Creation Date: Jun 03, 2003
 */
public class XMLReader extends ETLReader implements DefaultReaderCore {

    private DocumentBuilder mBuilder;
    private XMLHandler xmlHandler = null;
    private boolean validating;
    private boolean namespaceAware;
    private String docBuilder;

    private static final String XPATH_EVALUATE_ATTRIB = "XPATHEVALUATE";
    private static final String DOCUMENT_BUILDER = "DOCUMENTBUILDER";

    public XMLReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        try {

            this.docBuilder = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), DOCUMENT_BUILDER, null);
            this.validating = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "VALIDATE", false);
            this.namespaceAware = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "NAMESPACEAWARE", false);

            if (docBuilder != null) {
                this.xmlHandler = (XMLHandler) Class.forName(docBuilder).newInstance();
                this.mBuilder = this.xmlHandler.getDocumentBuilder(this.validating, this.namespaceAware);
            }
            else {
                DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
                configureDocumentBuilderFactory(dmf);
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "DOM Parser engine - "
                        + dmf.getClass().getCanonicalName());
                this.mBuilder = dmf.newDocumentBuilder();
            }

        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

    }

    ArrayList xmlFiles = new ArrayList();
    /**
     * ******************************************88/
     */

    public static final String XPATH_ATTRIB = "XPATH";
    private String mRootXPath;
    public static String FORMAT_STRING = "FORMATSTRING";

    class XMLETLOutPort extends ETLOutPort {

        final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
            if (this.xpath == null || this.getXMLConfig().hasAttribute("DATATYPE") == false)
                ((Element) this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
            this.setPortClass();
        }

        boolean fetchAttribute = false;
        XPathExpression mXPathExp;
        String fmt, xpath;
        Format formatter;
        String attribute = null;
        boolean mbXPathEvaluateField;
        String[] mRecursiveXPath;
        ParsePosition position;

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), XPATH_ATTRIB, null);
            this.mbXPathEvaluateField = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
                    XPATH_EVALUATE_ATTRIB, true);
            int res = super.initialize(xmlConfig);

            if (res != 0)
                return res;

            fmt = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), FORMAT_STRING, null);

            if (xpath != null) {

                if (this.mbXPathEvaluateField) {
                    try {
                        XPath tmp;
                        if (xmlHandler != null) {
                            tmp = xmlHandler.getNewXPath();
                        }
                        else {
                            XPathFactory xpf = XPathFactory.newInstance();
                            tmp = xpf.newXPath();
                        }
                        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - "
                                + tmp.getClass().getCanonicalName());

                        mXPathExp = tmp.compile(xpath);
                    } catch (Exception e) {
                        throw new KETLThreadException(e.toString(), this);
                    }
                }
                else {
                    if (xpath.contains("/")) {
                        this.mRecursiveXPath = xpath.split("/");
                        if (this.mRecursiveXPath[this.mRecursiveXPath.length - 1].startsWith("@")) {
                            fetchAttribute = true;
                            attribute = this.mRecursiveXPath[this.mRecursiveXPath.length - 1].substring(1);
                        }
                    }
                    else {
                        fetchAttribute = xpath.startsWith("@");
                        if (fetchAttribute) {
                            attribute = xpath.substring(1);
                        }
                    }

                }
            }

            return 0;

        }

        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }

        public XMLETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    private XPathExpression mXPath;

    class XPathHolder {

        FastSimpleDateFormat fmt = null;
        int datatype;
        String mXPath;
        boolean fetchAttribute = false;
        XPathExpression mXPathExpression;
        ParsePosition position = new ParsePosition(0);
    }

    private boolean mbXPathEvaluateNodes = true;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.transformation.SubComponentParallelTransform#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node xmlConfig) throws KETLThreadException {

        int res = super.initialize(xmlConfig);

        if ((this.mRootXPath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), XPATH_ATTRIB, null)) == null) {
            // No TABLE attribute listed...
            throw new KETLThreadException("ERROR: No root XPATH attribute specified in step '" + this.getName() + "'.", this);
        }

        this.mbXPathEvaluateNodes = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), XPATH_EVALUATE_ATTRIB,
                this.mbXPathEvaluateNodes);

        if (mbXPathEvaluateNodes) {
            try {
                XPath tmp;
                if (xmlHandler != null)
                    tmp = xmlHandler.getNewXPath();
                else {
                    XPathFactory xpf = XPathFactory.newInstance();
                    tmp = xpf.newXPath();
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - "
                            + xpf.getClass().getCanonicalName());
                }
                this.mXPath = tmp.compile(mRootXPath);
            } catch (Exception e) {
                throw new KETLThreadException(e, this);
            }
        }

        try {
            if (getFiles() == false) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,"No files found");

                for (int i = 0; i < this.maParameters.size(); i++) {
                    String searchPath = this.getParameterValue(i, SEARCHPATH);
                    ResourcePool.LogMessage(this,ResourcePool.ERROR_MESSAGE, "Search path(s): " + searchPath);
                }

                throw new KETLThreadException("No files found, check search paths", this);
            }
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        return res;
    }

    public static String SEARCHPATH = "SEARCHPATH";

    private boolean getFiles() throws Exception {

        ArrayList files = new ArrayList();
        for (int i = 0; i < this.maParameters.size(); i++) {
            String[] fileNames = FileTools.getFilenames(this.getParameterValue(i, SEARCHPATH));

            if (fileNames != null)
                java.util.Collections.addAll(files, (Object[]) fileNames);
        }

        if (files.size() == 0)
            return false;

        for (int i = 0; i < files.size(); i++) {
            if (i % this.partitions == this.partitionID)
                this.xmlFiles.add(files.get(i));
        }

        return true;
    }

    Node currentNode = null;

    public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException {

        while (nodeList == null && this.xmlFiles.size() > 0) {
            this.loadNodeList((String) this.xmlFiles.remove(0));
        }

        if (nodeList == null)
            return COMPLETE;

        currentNode = (Node) nodeList.get(nodePos++);

        int pos = 0;
        for (int i = 0; i < this.mOutPorts.length; i++) {
            if (this.mOutPorts[i].isUsed()) {
                if (this.mOutPorts[i].isConstant())
                    pResultArray[pos++] = this.mOutPorts[i].getConstantValue();
                else {
                    try {
                        pResultArray[pos++] = this.getXMLValue(i);
                    } catch (Exception e) {
                        throw new KETLReadException(e);
                    }
                }
            }
        }

        if (nodePos >= nodeListLength) {
            this.nodeList = null;
        }

        return 1;
    }

    List nodeList = null;
    int itemPos = 0;

    private static List convertToList(NodeList list, List oldList) {

        if (oldList == null)
            oldList = new ArrayList(list == null ? 0 : list.getLength());
        else {
            oldList.clear();
        }

        for (int i = list.getLength() - 1; i >= 0; i--) {
            oldList.add(list.item(i));
        }

        return oldList;
    }

    public void loadNodeList(String file) throws KETLReadException {
        try {
            doc = mBuilder.parse(new File(file));
            if (this.mbXPathEvaluateNodes) {
                if (this.xmlHandler == null)
                    nodeList = convertToList((NodeList) this.mXPath.evaluate(doc, XPathConstants.NODESET), nodeList);
                else
                    nodeList = this.xmlHandler.evaluateXPath(this.mXPath, doc, nodeList);
            }
            else {
                nodeList = convertToList(doc.getElementsByTagName(mRootXPath), nodeList);
            }

            nodeListLength = nodeList.size();
            nodePos = 0;

            if (nodeListLength == 0) {
                nodeList = null;
            }
        } catch (Exception e) {
            if (e instanceof KETLReadException)
                throw (KETLReadException) e;

            throw new KETLReadException(e);
        }
    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new XMLETLOutPort(this, srcStep);
    }

    @Override
    protected void close(boolean success) {
    }

    private void configureDocumentBuilderFactory(DocumentBuilderFactory dmf) {
        if (dmf.isNamespaceAware()) {
            ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Parser is namespace aware");
        }
        if (dmf.isValidating()) {
            ResourcePool.LogMessage(this,ResourcePool.INFO_MESSAGE, "Parser is validating");
        }

        dmf.setValidating(this.validating);
        dmf.setNamespaceAware(this.namespaceAware);
    }

    private Document doc;
    private int nodePos = 0, nodeListLength;

    public Object getXMLValue(int i) throws KETLReadException {

        XMLETLOutPort port = (XMLETLOutPort) this.mOutPorts[i];

        try {
            String result = null;

            Node cur = (Node) currentNode;
            if (port.mbXPathEvaluateField)
                result = (String) port.mXPathExp.evaluate(cur, XPathConstants.STRING);
            else {
                if (port.mRecursiveXPath == null) {
                    if (port.fetchAttribute)
                        result = XMLHelper.getAttributeAsString(cur.getAttributes(), port.attribute, null);
                    else
                        result = XMLHelper.getChildNodeValueAsString(cur, port.xpath, null, null, null);
                }
                else {
                    Node node = port.mRecursiveXPath[0].equals("") ? doc : cur;
                    int len = port.fetchAttribute ? port.mRecursiveXPath.length - 1 : port.mRecursiveXPath.length;

                    for (int x = 0; x < len; x++) {
                        if (port.mRecursiveXPath[x].equals(""))
                            continue;
                        node = XMLHelper.getElementByName(node, port.mRecursiveXPath[x], "*", "*");
                        if (node == null)
                            return null;
                    }

                    if (port.fetchAttribute) {
                        result = XMLHelper.getAttributeAsString(node.getAttributes(), port.attribute, null);
                    }
                    else
                        result = XMLHelper.getTextContent(node);
                }

            }

            if (result == null || result.length() == 0)
                return null;

            Class cl = port.getPortClass();
            if (cl == Float.class || cl == float.class)
                return Float.parseFloat(result);

            if (cl == String.class)
                return result;

            if (cl == Long.class || cl == long.class)
                return Long.parseLong(result);

            if (cl == Integer.class || cl == int.class)
                return Integer.parseInt(result);

            if (cl == java.util.Date.class) {
                if (port.formatter == null) {
                    if (port.fmt != null)
                        port.formatter = new FastSimpleDateFormat(port.fmt);
                    else
                        port.formatter = new FastSimpleDateFormat();

                    port.position = new ParsePosition(0);
                }

                port.position.setIndex(0);
                return port.formatter.parseObject(result, port.position);
            }

            if (cl == Double.class || cl == double.class)
                return Double.parseDouble(result);

            if (cl == Character.class || cl == char.class)
                return new Character(result.charAt(0));

            if (cl == Boolean.class || cl == boolean.class)
                return Boolean.parseBoolean(result);

            if (cl == Byte[].class || cl == byte[].class)
                return result.getBytes();

            Constructor con;
            try {
                con = cl.getConstructor(new Class[] { String.class });
            } catch (Exception e) {
                throw new KETLTransformException("No constructor found for class " + cl.getCanonicalName()
                        + " that accepts a single string");
            }
            return con.newInstance(new Object[] { result });

        } catch (Exception e) {
            throw new KETLReadException("XML parsing failed for port " + port.mstrName, e);
        }
    }

}
