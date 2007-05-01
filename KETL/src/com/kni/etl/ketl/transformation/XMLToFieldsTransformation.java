/*
 * Created on Jul 13, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
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
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
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

// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

public class XMLToFieldsTransformation extends ETLTransformation {

    private static final String XPATH_EVALUATE_ATTRIB = "XPATHEVALUATE";
    private static final String DUMPXML_ATTRIB = "DUMPXML";
    private static final String DOCUMENT_BUILDER = "DOCUMENTBUILDER";

    private String docBuilder;
    private Transformer xmlTransformer;

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        if ((this.mRootXPath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), XPATH_ATTRIB, null)) == null) {
            // No TABLE attribute listed...
            throw new KETLThreadException("ERROR: No root XPATH attribute specified in step '" + this.getName() + "'.",
                    this);
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
            TransformerFactory tf = TransformerFactory.newInstance();
            xmlTransformer = tf.newTransformer();
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        return 0;
    }

    private DocumentBuilder mBuilder;
    private XMLHandler xmlHandler = null;
    private boolean validating;
    private boolean namespaceAware;

    public XMLToFieldsTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
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

    public static final String XPATH_ATTRIB = "XPATH";
    private boolean mbXPathEvaluateNodes = true;
    private String mRootXPath;

    XMLETLInPort xmlSrcPort = null;

    class XMLETLInPort extends ETLInPort {

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            if (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "XMLDATA", false)) {
                if (xmlSrcPort != null)
                    throw new KETLThreadException("Only one port can be assigned as XMLData", this);
                xmlSrcPort = this;
            }

            return 0;
        }

        public XMLETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new XMLETLOutPort(this, srcStep);
    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new XMLETLInPort(this, srcStep);
    }

    public static String FORMAT_STRING = "FORMATSTRING";

    class XMLETLOutPort extends ETLOutPort {

        final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
            if (this.xpath == null || this.getXMLConfig().hasAttribute("DATATYPE") == false)
                ((Element) this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
            this.setPortClass();
        }

        @Override
        public ETLPort getAssociatedInPort() throws KETLThreadException {
            if (xpath != null)
                return xmlSrcPort;

            return super.getAssociatedInPort();
        }

        boolean fetchAttribute = false;
        XPathExpression mXPathExp;
        String fmt, xpath;
        Format formatter;
        String attribute = null;
        boolean mbXPathEvaluateField;
        String[] mRecursiveXPath;
        ParsePosition position;
        boolean mDumpXML = false;
        String nullIF = null;

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), XPATH_ATTRIB, null);

            if (this.xpath == null)
                this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getParentNode().getAttributes(), XPATH_ATTRIB,
                        null);

            this.mDumpXML = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), DUMPXML_ATTRIB, false);

            this.nullIF = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NULLIF", null);
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

        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            if (this.xpath == null || this.isConstant() || this.isUsed() == false)
                return super.generateCode(portReferenceIndex);

            // must be pure code then do some replacing

            return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = (("
                    + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).getXMLValue("
                    + portReferenceIndex + ")";

        }

        public XMLETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        if (this.xmlSrcPort == null)
            return super.getRecordExecuteMethodFooter();

        return " return ((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).noMoreNodes()?SUCCESS:REPEAT_RECORD;}";
    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        if (this.xmlSrcPort == null)
            return super.getRecordExecuteMethodHeader();

        return super.getRecordExecuteMethodHeader() + " if(((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).loadNodeList(" + this.xmlSrcPort.generateReference()
                + ") == false) return SKIP_RECORD;";
    }

    public boolean noMoreNodes() {

        if (pos == length) {
            this.currentXMLString = null;
            return true;
        }
        return false;
    }
    
    private String getXMLDump(Object o) throws Exception{
        
        if(o== null) return null;
        if (o instanceof Node)
            return XMLHelper.outputXML((Node) o);
        
        if (o instanceof Source) {
            StringWriter ws = new StringWriter();
            xmlTransformer.transform((Source) o, new StreamResult(ws));
            return ws.toString();
        }
        
        throw new Exception("Object could not be converted to xml " + o.getClass().getCanonicalName());
    }

    public Object getXMLValue(int i) throws KETLTransformException {
        XMLETLOutPort port = (XMLETLOutPort) this.mOutPorts[i];

        try {
            String result = null;

            Node cur = (Node) currentNode;
            if (port.mbXPathEvaluateField) {
                if (port.mDumpXML) {
                    result = getXMLDump( port.mXPathExp.evaluate(cur, XPathConstants.NODE));

                }
                else
                    result = (String) port.mXPathExp.evaluate(cur, XPathConstants.STRING);
            }
            else {
                if (port.mRecursiveXPath == null) {
                    if (port.mDumpXML) {
                        result = getXMLDump(cur);
                    }
                    else if (port.fetchAttribute)
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

                    if (port.mDumpXML) {
                        result = getXMLDump(node);
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

    private String currentXMLString;
    private Document doc;
    private Node currentNode;
    private List nodeList;
    private XPathExpression mXPath;
    private int pos = 0, length;

    public boolean loadNodeList(String string) throws KETLTransformException {
        try {
            if (this.currentXMLString == null || this.currentXMLString.equals(string) == false) {

                if (string == null)
                    return false;

                doc = mBuilder.parse(new InputSource(new StringReader(string)));
                if (this.mbXPathEvaluateNodes) {
                    if (this.xmlHandler == null)
                        nodeList = convertToList((NodeList) this.mXPath.evaluate(doc, XPathConstants.NODESET), nodeList);
                    else
                        nodeList = this.xmlHandler.evaluateXPath(this.mXPath, doc, nodeList);
                }
                else {
                    nodeList = convertToList(doc.getElementsByTagName(mRootXPath), nodeList);
                }

                length = nodeList.size();
                pos = 0;
                if (length == 0)
                    return false;

                this.currentXMLString = string;
            }

            currentNode = (Node) nodeList.get(pos++);

            return true;
        } catch (Exception e) {
            if (e instanceof KETLTransformException)
                throw (KETLTransformException) e;

            throw new KETLTransformException(e);
        }
    }

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

    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub

    }

}
