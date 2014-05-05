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

// TODO: Auto-generated Javadoc
/**
 * The Class XMLReader.
 * 
 * @author bsullivan Creation Date: Jun 03, 2003
 */
public class XMLReader extends ETLReader implements DefaultReaderCore {

	@Override
	protected String getVersion() {
		return "$LastChangedRevision$";
	}

	/** The builder. */
	private DocumentBuilder mBuilder;

	/** The xml handler. */
	private XMLHandler xmlHandler = null;

	/** The validating. */
	private boolean validating;

	/** The namespace aware. */
	private boolean namespaceAware;

	/** The doc builder. */
	private String docBuilder;

	/** The Constant XPATH_EVALUATE_ATTRIB. */
	private static final String XPATH_EVALUATE_ATTRIB = "XPATHEVALUATE";

	/** The Constant DOCUMENT_BUILDER. */
	private static final String DOCUMENT_BUILDER = "DOCUMENTBUILDER";

	/**
	 * Instantiates a new XML reader.
	 * 
	 * @param pXMLConfig
	 *            the XML config
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public XMLReader(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

		try {

			this.docBuilder = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), XMLReader.DOCUMENT_BUILDER, null);
			this.validating = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "VALIDATE", false);
			this.namespaceAware = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "NAMESPACEAWARE", false);

			if (this.docBuilder != null) {
				this.xmlHandler = (XMLHandler) Class.forName(this.docBuilder).newInstance();
				this.mBuilder = this.xmlHandler.getDocumentBuilder(this.validating, this.namespaceAware);
			} else {
				DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
				this.configureDocumentBuilderFactory(dmf);
				ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "DOM Parser engine - " + dmf.getClass().getCanonicalName());
				this.mBuilder = dmf.newDocumentBuilder();
			}

		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

	}

	/** The xml files. */
	ArrayList xmlFiles = new ArrayList();

	/** ******************************************88/. */

	public static final String XPATH_ATTRIB = "XPATH";

	/** The root X path. */
	private String mRootXPath;

	/** The FORMA t_ STRING. */
	public static String FORMAT_STRING = "FORMATSTRING";

	/**
	 * The Class XMLETLOutPort.
	 */
	class XMLETLOutPort extends ETLOutPort {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * com.kni.etl.ketl.ETLPort#setDataTypeFromPort(com.kni.etl.ketl.ETLPort
		 * )
		 */
		@Override
		final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
			if (this.xpath == null || this.getXMLConfig().hasAttribute("DATATYPE") == false)
				(this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
			this.setPortClass();
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
			this.xpath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), XMLReader.XPATH_ATTRIB, null);
			this.mbXPathEvaluateField = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), XMLReader.XPATH_EVALUATE_ATTRIB, true);
			int res = super.initialize(xmlConfig);

			if (res != 0)
				return res;

			this.fmt = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), XMLReader.FORMAT_STRING, null);

			if (this.xpath != null) {

				if (this.mbXPathEvaluateField) {
					try {
						XPath tmp;
						if (XMLReader.this.xmlHandler != null) {
							tmp = XMLReader.this.xmlHandler.getNewXPath();
						} else {
							XPathFactory xpf = XPathFactory.newInstance();
							tmp = xpf.newXPath();
						}
						ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - " + tmp.getClass().getCanonicalName());

						this.mXPathExp = tmp.compile(this.xpath);
					} catch (Exception e) {
						throw new KETLThreadException(e.toString(), this);
					}
				} else {
					if (this.xpath.contains("/")) {
						this.mRecursiveXPath = this.xpath.split("/");
						if (this.mRecursiveXPath[this.mRecursiveXPath.length - 1].startsWith("@")) {
							this.fetchAttribute = true;
							this.attribute = this.mRecursiveXPath[this.mRecursiveXPath.length - 1].substring(1);
						}
					} else {
						this.fetchAttribute = this.xpath.startsWith("@");
						if (this.fetchAttribute) {
							this.attribute = this.xpath.substring(1);
						}
					}

				}
			}

			return 0;

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLPort#containsCode()
		 */
		@Override
		public boolean containsCode() throws KETLThreadException {
			return true;
		}

		/**
		 * Instantiates a new XMLETL out port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public XMLETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

	}

	/** The X path. */
	private XPathExpression mXPath;

	/**
	 * The Class XPathHolder.
	 */
	class XPathHolder {

		/** The fmt. */
		FastSimpleDateFormat fmt = null;

		/** The datatype. */
		int datatype;

		/** The X path. */
		String mXPath;

		/** The fetch attribute. */
		boolean fetchAttribute = false;

		/** The X path expression. */
		XPathExpression mXPathExpression;

		/** The position. */
		ParsePosition position = new ParsePosition(0);
	}

	/** The mb X path evaluate nodes. */
	private boolean mbXPathEvaluateNodes = true;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.transformation.SubComponentParallelTransform#initialize
	 * (org.w3c.dom.Node)
	 */
	@Override
	public int initialize(Node xmlConfig) throws KETLThreadException {

		int res = super.initialize(xmlConfig);

		if ((this.mRootXPath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), XMLReader.XPATH_ATTRIB, null)) == null) {
			// No TABLE attribute listed...
			throw new KETLThreadException("ERROR: No root XPATH attribute specified in step '" + this.getName() + "'.", this);
		}

		this.mbXPathEvaluateNodes = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), XMLReader.XPATH_EVALUATE_ATTRIB, this.mbXPathEvaluateNodes);

		if (this.mbXPathEvaluateNodes) {
			try {
				XPath tmp;
				if (this.xmlHandler != null)
					tmp = this.xmlHandler.getNewXPath();
				else {
					XPathFactory xpf = XPathFactory.newInstance();
					tmp = xpf.newXPath();
					ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "XPath engine - " + xpf.getClass().getCanonicalName());
				}
				this.mXPath = tmp.compile(this.mRootXPath);
			} catch (Exception e) {
				throw new KETLThreadException(e, this);
			}
		}

		try {
			if (this.getFiles() == false) {
				ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "No files found");

				for (int i = 0; i < this.maParameters.size(); i++) {
					String searchPath = this.getParameterValue(i, XMLReader.SEARCHPATH);
					ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Search path(s): " + searchPath);
				}

				throw new KETLThreadException("No files found, check search paths", this);
			}
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

		return res;
	}

	/** The SEARCHPATH. */
	public static String SEARCHPATH = "SEARCHPATH";

	/**
	 * Gets the files.
	 * 
	 * @return the files
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private boolean getFiles() throws Exception {

		ArrayList files = new ArrayList();
		for (int i = 0; i < this.maParameters.size(); i++) {
			String[] fileNames = FileTools.getFilenames(this.getParameterValue(i, XMLReader.SEARCHPATH));

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

	/** The current node. */
	Node currentNode = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.DefaultReaderCore#getNextRecord(java.lang.Object[],
	 * java.lang.Class[], int)
	 */
	public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLReadException {

		while (this.nodeList == null && this.xmlFiles.size() > 0) {
			this.loadNodeList((String) this.xmlFiles.remove(0));
		}

		if (this.nodeList == null)
			return DefaultReaderCore.COMPLETE;

		this.currentNode = (Node) this.nodeList.get(this.nodePos++);

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

		if (this.nodePos >= this.nodeListLength) {
			this.nodeList = null;
		}

		return 1;
	}

	/** The node list. */
	List nodeList = null;

	/** The item pos. */
	int itemPos = 0;

	/**
	 * Convert to list.
	 * 
	 * @param list
	 *            the list
	 * @param oldList
	 *            the old list
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

	/**
	 * Load node list.
	 * 
	 * @param file
	 *            the file
	 * 
	 * @throws KETLReadException
	 *             the KETL read exception
	 */
	public void loadNodeList(String file) throws KETLReadException {
		try {
			this.doc = this.mBuilder.parse(new File(file));
			if (this.mbXPathEvaluateNodes) {
				if (this.xmlHandler == null)
					this.nodeList = XMLReader.convertToList((NodeList) this.mXPath.evaluate(this.doc, XPathConstants.NODESET), this.nodeList);
				else
					this.nodeList = this.xmlHandler.evaluateXPath(this.mXPath, this.doc, this.nodeList);
			} else {
				this.nodeList = XMLReader.convertToList(this.doc.getElementsByTagName(this.mRootXPath), this.nodeList);
			}

			this.nodeListLength = this.nodeList.size();
			this.nodePos = 0;

			if (this.nodeListLength == 0) {
				this.nodeList = null;
			}
		} catch (Exception e) {
			if (e instanceof KETLReadException)
				throw (KETLReadException) e;

			throw new KETLReadException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLOutPort getNewOutPort(ETLStep srcStep) {
		return new XMLETLOutPort(this, srcStep);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success, boolean jobFailed) {
	}

	/**
	 * Configure document builder factory.
	 * 
	 * @param dmf
	 *            the dmf
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

	/** The doc. */
	private Document doc;

	/** The node list length. */
	private int nodePos = 0, nodeListLength;

	/**
	 * Gets the XML value.
	 * 
	 * @param i
	 *            the i
	 * 
	 * @return the XML value
	 * 
	 * @throws KETLReadException
	 *             the KETL read exception
	 */
	public Object getXMLValue(int i) throws KETLReadException {

		XMLETLOutPort port = (XMLETLOutPort) this.mOutPorts[i];

		try {
			String result = null;

			Node cur = this.currentNode;
			if (port.mbXPathEvaluateField)
				result = (String) port.mXPathExp.evaluate(cur, XPathConstants.STRING);
			else {
				if (port.mRecursiveXPath == null) {
					if (port.fetchAttribute)
						result = XMLHelper.getAttributeAsString(cur.getAttributes(), port.attribute, null);
					else
						result = XMLHelper.getChildNodeValueAsString(cur, port.xpath, null, null, null);
				} else {
					Node node = port.mRecursiveXPath[0].equals("") ? this.doc : cur;
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
					} else
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
				throw new KETLTransformException("No constructor found for class " + cl.getCanonicalName() + " that accepts a single string");
			}
			return con.newInstance(new Object[] { result });

		} catch (Exception e) {
			throw new KETLReadException("XML parsing failed for port " + port.mstrName, e);
		}
	}

}
