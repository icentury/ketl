package com.kni.etl.util;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import net.sf.saxon.dom.DocumentBuilderFactoryImpl;
import net.sf.saxon.dom.NodeOverNodeInfo;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.xpath.XPathEvaluator;
import net.sf.saxon.xpath.XPathFactoryImpl;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;

public class SAXONXMLHandler extends XMLHandler {

    DocumentBuilder db;

    @Override
    public DocumentBuilder getDocumentBuilder(boolean validate, boolean nameSpaceAware) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, ParserConfigurationException {
        if (this.db == null) {
            DocumentBuilderFactoryImpl dmf = (DocumentBuilderFactoryImpl) Class.forName(
                    "net.sf.saxon.dom.DocumentBuilderFactoryImpl").newInstance();
            this.db = dmf.newDocumentBuilder();
            XMLHandler.configureDocumentBuilderFactory(dmf, validate, nameSpaceAware);
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "DOM engine - "
                    + dmf.getClass().getCanonicalName());
        }

        return this.db;
    }

    @Override
    public List evaluateXPath(XPathExpression path, Document node, List arg) throws XPathExpressionException {

        Object res = path.evaluate(node, XPathConstants.NODESET);
        if (res instanceof NodeList) {
            if (arg == null)
                arg = new ArrayList(((NodeList) res).getLength());
            else
                arg.clear();

            for (int i = ((NodeList) res).getLength() - 1; i >= 0; i--) {
                arg.add(((NodeList) res).item(i));
            }

            return arg;
        }

        ArrayList ar = new ArrayList(((List) res).size());
        for (Object o : (List) res) {
            ar.add(NodeOverNodeInfo.wrap((NodeInfo) o));
        }

        return ar;
    }

    XPathFactoryImpl xpf;

    @Override
    public XPath getNewXPath() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        if (this.xpf == null) {
            this.xpf = (XPathFactoryImpl) Class.forName("net.sf.saxon.xpath.XPathFactoryImpl").newInstance();
            ((net.sf.saxon.dom.DocumentBuilderImpl) this.db).setConfiguration(this.xpf.getConfiguration());

            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - "
                    + this.xpf.getClass().getCanonicalName());
        }
        XPathEvaluator tmp = (XPathEvaluator) this.xpf.newXPath();

        return tmp;
    }

}
