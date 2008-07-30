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

// TODO: Auto-generated Javadoc
/**
 * The Class SAXONXMLHandler.
 */
public class SAXONXMLHandler extends XMLHandler {

    /** The db. */
    DocumentBuilder db;

    /* (non-Javadoc)
     * @see com.kni.etl.util.XMLHandler#getDocumentBuilder(boolean, boolean)
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.util.XMLHandler#evaluateXPath(javax.xml.xpath.XPathExpression, org.w3c.dom.Document, java.util.List)
     */
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

    /** The xpf. */
    XPathFactoryImpl xpf;

    /* (non-Javadoc)
     * @see com.kni.etl.util.XMLHandler#getNewXPath()
     */
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
