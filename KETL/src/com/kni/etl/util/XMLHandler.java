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

// TODO: Auto-generated Javadoc
/**
 * The Class XMLHandler.
 */
abstract public class XMLHandler {

    /**
     * Gets the document builder.
     * 
     * @param validate the validate
     * @param nameSpaceAware the name space aware
     * 
     * @return the document builder
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws ClassNotFoundException the class not found exception
     * @throws ParserConfigurationException the parser configuration exception
     */
    abstract public DocumentBuilder getDocumentBuilder(boolean validate, boolean nameSpaceAware)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, ParserConfigurationException;

    /**
     * Gets the new X path.
     * 
     * @return the new X path
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws ClassNotFoundException the class not found exception
     */
    abstract public XPath getNewXPath() throws InstantiationException, IllegalAccessException, ClassNotFoundException;

    /**
     * Evaluate X path.
     * 
     * @param path the path
     * @param doc the doc
     * @param arg the arg
     * 
     * @return the list
     * 
     * @throws XPathExpressionException the x path expression exception
     */
    abstract public List evaluateXPath(XPathExpression path, Document doc, List arg) throws XPathExpressionException;

    /**
     * Configure document builder factory.
     * 
     * @param dmf the dmf
     * @param validate the validate
     * @param nameSpaceAware the name space aware
     */
    static void configureDocumentBuilderFactory(DocumentBuilderFactory dmf, boolean validate, boolean nameSpaceAware) {
        if (dmf.isNamespaceAware()) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Parser is namespace aware");
        }
        if (dmf.isValidating()) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Parser is validating");
        }

        dmf.setValidating(validate);
        dmf.setNamespaceAware(nameSpaceAware);
    }
}
