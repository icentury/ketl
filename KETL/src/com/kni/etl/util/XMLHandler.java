package com.kni.etl.util;

import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;

import com.kni.etl.dbutils.ResourcePool;

abstract public class XMLHandler {

    abstract public DocumentBuilder getDocumentBuilder(boolean validate, boolean nameSpaceAware) throws InstantiationException, IllegalAccessException, ClassNotFoundException, ParserConfigurationException;

    abstract public XPath getNewXPath() throws InstantiationException, IllegalAccessException, ClassNotFoundException;

    abstract public List evaluateXPath(XPathExpression path, Document doc,List arg) throws XPathExpressionException;

    static     void configureDocumentBuilderFactory(DocumentBuilderFactory dmf,boolean validate, boolean nameSpaceAware) {
        if(dmf.isNamespaceAware()) {
            ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.INFO_MESSAGE, "Parser is namespace aware");
        }
        if(dmf.isValidating()) {
            ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.INFO_MESSAGE, "Parser is validating");
        }
        
        dmf.setValidating(validate);
        dmf.setNamespaceAware(nameSpaceAware);
    }
}
