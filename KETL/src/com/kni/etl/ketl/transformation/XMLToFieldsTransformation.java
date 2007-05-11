/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.transformation;

import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.text.Format;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.XMLHandler;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

/**
 * The Class XMLToFieldsTransformation.
 */
public class XMLToFieldsTransformation extends ETLTransformation {

    /** The Constant XPATH_EVALUATE_ATTRIB. */
    private static final String XPATH_EVALUATE_ATTRIB = "XPATHEVALUATE";
    
    /** The Constant DUMPXML_ATTRIB. */
    private static final String DUMPXML_ATTRIB = "DUMPXML";
    
    /** The Constant DOCUMENT_BUILDER. */
    private static final String DOCUMENT_BUILDER = "DOCUMENTBUILDER";

    /** The doc builder. */
    private String docBuilder;
    
    /** The xml transformer. */
    private Transformer xmlTransformer;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLTransform#initialize(org.w3c.dom.Node)
     */
    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        if ((this.mRootXPath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                XMLToFieldsTransformation.XPATH_ATTRIB, null)) == null) {
            // No TABLE attribute listed...
            throw new KETLThreadException("ERROR: No root XPATH attribute specified in step '" + this.getName() + "'.",
                    this);
        }

        this.mbXPathEvaluateNodes = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
                XMLToFieldsTransformation.XPATH_EVALUATE_ATTRIB, this.mbXPathEvaluateNodes);

        if (this.mbXPathEvaluateNodes) {
            try {
                XPath tmp;
                if (this.xmlHandler != null)
                    tmp = this.xmlHandler.getNewXPath();
                else {
                    XPathFactory xpf = XPathFactory.newInstance();
                    tmp = xpf.newXPath();
                    ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - "
                            + xpf.getClass().getCanonicalName());

                }
                this.mXPath = tmp.compile(this.mRootXPath);
            } catch (Exception e) {
                throw new KETLThreadException(e, this);
            }
        }
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            this.xmlTransformer = tf.newTransformer();
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        return 0;
    }

    /** The builder. */
    private DocumentBuilder mBuilder;
    
    /** The xml handler. */
    private XMLHandler xmlHandler = null;
    
    /** The validating. */
    private boolean validating;
    
    /** The namespace aware. */
    private boolean namespaceAware;

    /**
     * Instantiates a new XML to fields transformation.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public XMLToFieldsTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        try {

            this.docBuilder = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(),
                    XMLToFieldsTransformation.DOCUMENT_BUILDER, null);
            this.validating = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "VALIDATE", false);
            this.namespaceAware = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "NAMESPACEAWARE", false);

            if (this.docBuilder != null) {
                this.xmlHandler = (XMLHandler) Class.forName(this.docBuilder).newInstance();
                this.mBuilder = this.xmlHandler.getDocumentBuilder(this.validating, this.namespaceAware);
            }
            else {
                DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
                this.configureDocumentBuilderFactory(dmf);
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "DOM Parser engine - "
                        + dmf.getClass().getCanonicalName());
                this.mBuilder = dmf.newDocumentBuilder();
            }

        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

    }

    /**
     * Configure document builder factory.
     * 
     * @param dmf the dmf
     */
    private void configureDocumentBuilderFactory(DocumentBuilderFactory dmf) {
        if (dmf.isNamespaceAware()) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Parser is namespace aware");
        }
        if (dmf.isValidating()) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Parser is validating");
        }

        dmf.setValidating(this.validating);
        dmf.setNamespaceAware(this.namespaceAware);
    }

    /** The Constant XPATH_ATTRIB. */
    public static final String XPATH_ATTRIB = "XPATH";
    
    /** The mb X path evaluate nodes. */
    private boolean mbXPathEvaluateNodes = true;
    
    /** The root X path. */
    private String mRootXPath;

    /** The xml src port. */
    XMLETLInPort xmlSrcPort = null;

    /**
     * The Class XMLETLInPort.
     */
    class XMLETLInPort extends ETLInPort {

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            if (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "XMLDATA", false)) {
                if (XMLToFieldsTransformation.this.xmlSrcPort != null)
                    throw new KETLThreadException("Only one port can be assigned as XMLData", this);
                XMLToFieldsTransformation.this.xmlSrcPort = this;
            }

            return 0;
        }

        /**
         * Instantiates a new XMLETL in port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public XMLETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new XMLETLOutPort(this, srcStep);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
     */
    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new XMLETLInPort(this, srcStep);
    }

    /** The FORMA t_ STRING. */
    public static String FORMAT_STRING = "FORMATSTRING";

    /**
     * The Class XMLETLOutPort.
     */
    class XMLETLOutPort extends ETLOutPort {

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#setDataTypeFromPort(com.kni.etl.ketl.ETLPort)
         */
        @Override
        final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
            if (this.xpath == null || this.getXMLConfig().hasAttribute("DATATYPE") == false)
                (this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
            this.setPortClass();
        }

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#getAssociatedInPort()
         */
        @Override
        public ETLPort getAssociatedInPort() throws KETLThreadException {
            if (this.xpath != null)
                return XMLToFieldsTransformation.this.xmlSrcPort;

            return super.getAssociatedInPort();
        }

        /** The fetch attribute. */
        boolean fetchAttribute = false;
        
        /** The X path exp. */
        XPathExpression mXPathExp;
        
        /** The xpath. */
        String fmt, xpath;
        
        /** The formatter. */
        Format formatter;
        
        /** The attribute. */
        String attribute = null;
        
        /** The mb X path evaluate field. */
        boolean mbXPathEvaluateField;
        
        /** The recursive X path. */
        String[] mRecursiveXPath;
        
        /** The position. */
        ParsePosition position;
        
        /** The dump XML. */
        boolean mDumpXML = false;
        
        /** The null IF. */
        String nullIF = null;

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
         */
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                    XMLToFieldsTransformation.XPATH_ATTRIB, null);

            if (this.xpath == null)
                this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getParentNode().getAttributes(),
                        XMLToFieldsTransformation.XPATH_ATTRIB, null);

            this.mDumpXML = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
                    XMLToFieldsTransformation.DUMPXML_ATTRIB, false);

            this.nullIF = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NULLIF", null);
            this.mbXPathEvaluateField = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
                    XMLToFieldsTransformation.XPATH_EVALUATE_ATTRIB, true);
            int res = super.initialize(xmlConfig);

            if (res != 0)
                return res;

            this.fmt = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
                    XMLToFieldsTransformation.FORMAT_STRING, null);

            if (this.xpath != null) {

                if (this.mbXPathEvaluateField) {
                    try {
                        XPath tmp;
                        if (XMLToFieldsTransformation.this.xmlHandler != null) {
                            tmp = XMLToFieldsTransformation.this.xmlHandler.getNewXPath();
                        }
                        else {
                            XPathFactory xpf = XPathFactory.newInstance();
                            tmp = xpf.newXPath();
                        }
                        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - "
                                + tmp.getClass().getCanonicalName());

                        this.mXPathExp = tmp.compile(this.xpath);
                    } catch (Exception e) {
                        throw new KETLThreadException(e.toString(), this);
                    }
                }
                else {
                    if (this.xpath.contains("/")) {
                        this.mRecursiveXPath = this.xpath.split("/");
                        if (this.mRecursiveXPath[this.mRecursiveXPath.length - 1].startsWith("@")) {
                            this.fetchAttribute = true;
                            this.attribute = this.mRecursiveXPath[this.mRecursiveXPath.length - 1].substring(1);
                        }
                    }
                    else {
                        this.fetchAttribute = this.xpath.startsWith("@");
                        if (this.fetchAttribute) {
                            this.attribute = this.xpath.substring(1);
                        }
                    }

                }
            }

            return 0;

        }

        /* (non-Javadoc)
         * @see com.kni.etl.ketl.ETLOutPort#generateCode(int)
         */
        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            if (this.xpath == null || this.isConstant() || this.isUsed() == false)
                return super.generateCode(portReferenceIndex);

            // must be pure code then do some replacing

            return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = (("
                    + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).getXMLValue("
                    + portReferenceIndex + ")";

        }

        /**
         * Instantiates a new XMLETL out port.
         * 
         * @param esOwningStep the es owning step
         * @param esSrcStep the es src step
         */
        public XMLETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodFooter()
     */
    @Override
    protected String getRecordExecuteMethodFooter() {
        if (this.xmlSrcPort == null)
            return super.getRecordExecuteMethodFooter();

        return " return ((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).noMoreNodes()?SUCCESS:REPEAT_RECORD;}";
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodHeader()
     */
    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        if (this.xmlSrcPort == null)
            return super.getRecordExecuteMethodHeader();

        return super.getRecordExecuteMethodHeader() + " if(((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).loadNodeList(" + this.xmlSrcPort.generateReference()
                + ") == false) return SKIP_RECORD;";
    }

    /**
     * No more nodes.
     * 
     * @return true, if successful
     */
    public boolean noMoreNodes() {

        if (this.pos == this.length) {
            this.currentXMLString = null;
            return true;
        }
        return false;
    }

    /**
     * Gets the XML dump.
     * 
     * @param o the o
     * 
     * @return the XML dump
     * 
     * @throws Exception the exception
     */
    private String getXMLDump(Object o) throws Exception {

        if (o == null)
            return null;
        if (o instanceof Node)
            return XMLHelper.outputXML((Node) o);

        if (o instanceof Source) {
            StringWriter ws = new StringWriter();
            this.xmlTransformer.transform((Source) o, new StreamResult(ws));
            return ws.toString();
        }

        throw new Exception("Object could not be converted to xml " + o.getClass().getCanonicalName());
    }

    /**
     * Gets the XML value.
     * 
     * @param i the i
     * 
     * @return the XML value
     * 
     * @throws KETLTransformException the KETL transform exception
     */
    public Object getXMLValue(int i) throws KETLTransformException {
        XMLETLOutPort port = (XMLETLOutPort) this.mOutPorts[i];

        try {
            String result = null;

            Node cur = this.currentNode;
            if (port.mbXPathEvaluateField) {
                if (port.mDumpXML) {
                    result = this.getXMLDump(port.mXPathExp.evaluate(cur, XPathConstants.NODE));

                }
                else
                    result = (String) port.mXPathExp.evaluate(cur, XPathConstants.STRING);
            }
            else {
                if (port.mRecursiveXPath == null) {
                    if (port.mDumpXML) {
                        result = this.getXMLDump(cur);
                    }
                    else if (port.fetchAttribute)
                        result = XMLHelper.getAttributeAsString(cur.getAttributes(), port.attribute, null);
                    else
                        result = XMLHelper.getChildNodeValueAsString(cur, port.xpath, null, null, null);
                }
                else {
                    Node node = port.mRecursiveXPath[0].equals("") ? this.doc : cur;
                    int len = port.fetchAttribute ? port.mRecursiveXPath.length - 1 : port.mRecursiveXPath.length;

                    for (int x = 0; x < len; x++) {
                        if (port.mRecursiveXPath[x].equals(""))
                            continue;
                        node = XMLHelper.getElementByName(node, port.mRecursiveXPath[x], "*", "*");
                        if (node == null)
                            return null;
                    }

                    if (port.mDumpXML) {
                        result = this.getXMLDump(node);
                    }
                    else if (port.fetchAttribute) {
                        result = XMLHelper.getAttributeAsString(node.getAttributes(), port.attribute, null);
                    }
                    else
                        result = XMLHelper.getTextContent(node);
                }

            }

            if (result == null || result.length() == 0 || (port.nullIF != null && port.nullIF.equals(result)))
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
            throw new KETLTransformException("XML parsing failed for port " + port.mstrName, e);
        }
    }

    /** The current XML string. */
    private String currentXMLString;
    
    /** The doc. */
    private Document doc;
    
    /** The current node. */
    private Node currentNode;
    
    /** The node list. */
    private List nodeList;
    
    /** The X path. */
    private XPathExpression mXPath;
    
    /** The length. */
    private int pos = 0, length;

    /**
     * Load node list.
     * 
     * @param string the string
     * 
     * @return true, if successful
     * 
     * @throws KETLTransformException the KETL transform exception
     */
    public boolean loadNodeList(String string) throws KETLTransformException {
        try {
            if (this.currentXMLString == null || this.currentXMLString.equals(string) == false) {

                if (string == null)
                    return false;

                this.doc = this.mBuilder.parse(new InputSource(new StringReader(string)));
                if (this.mbXPathEvaluateNodes) {
                    if (this.xmlHandler == null)
                        this.nodeList = XMLToFieldsTransformation.convertToList((NodeList) this.mXPath.evaluate(
                                this.doc, XPathConstants.NODESET), this.nodeList);
                    else
                        this.nodeList = this.xmlHandler.evaluateXPath(this.mXPath, this.doc, this.nodeList);
                }
                else {
                    this.nodeList = XMLToFieldsTransformation.convertToList(this.doc
                            .getElementsByTagName(this.mRootXPath), this.nodeList);
                }

                this.length = this.nodeList.size();
                this.pos = 0;
                if (this.length == 0)
                    return false;

                this.currentXMLString = string;
            }

            this.currentNode = (Node) this.nodeList.get(this.pos++);

            return true;
        } catch (Exception e) {
            if (e instanceof KETLTransformException)
                throw (KETLTransformException) e;

            throw new KETLTransformException(e);
        }
    }

    /**
     * Convert to list.
     * 
     * @param list the list
     * @param oldList the old list
     * 
     * @return the list
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub

    }

}
