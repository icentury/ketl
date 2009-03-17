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
package com.kni.etl.ketl;

import java.lang.reflect.Constructor;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.DataItemHelper;
import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.qa.QAItemLevelEventGenerator;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (3/5/2002 3:41:34 PM)
 * 
 * @author: Administrator
 */
public abstract class ETLPort {

	/** The mb QA items exist. */
	protected boolean mbQAItemsExist = false;

	/** The Constant NAME_ATTRIB. */
	public static final String NAME_ATTRIB = "NAME";

	/** The Constant OBJECT_TYPE_ATTRIB. */
	public static final String OBJECT_TYPE_ATTRIB = "OBJECTTYPE";

	/** The mstr name. */
	public String mstrName;

	/** The object type. */
	public String mObjectType;

	/** The data type. */
	private Class mDataType;

	/** The used. */
	private boolean mUsed = false;

	/** The node. */
	Node mNode;

	/** The mes src step. */
	public ETLStep mesStep, mesSrcStep;

	/** The ma QA event generators. */
	QAItemLevelEventGenerator[] maQAEventGenerators = null;

	/**
	 * Generate code.
	 * 
	 * @param portReferenceIndex
	 *            the port reference index
	 * 
	 * @return the string
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public String generateCode(int portReferenceIndex) throws KETLThreadException {
		throw new KETLThreadException("generateCode needs to be overridden for ETLWriter based steps", this);
	}

	/**
	 * Gets the port name.
	 * 
	 * @return the port name
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	abstract public String getPortName() throws KETLThreadException;

	/**
	 * Used.
	 * 
	 * @param arg0
	 *            the arg0
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public void used(boolean arg0) throws KETLThreadException {
		//ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Port: " + this.getPortName()
		//		+ (arg0 ? " enabled" : " disabled"));
		this.mUsed = arg0;
	}

	/**
	 * Checks if is used.
	 * 
	 * @return true, if is used
	 */
	public boolean isUsed() {
		return this.mUsed;
	}

	/**
	 * Sets the QA event generators.
	 * 
	 * @param aQAEventGenerators
	 *            the new QA event generators
	 */
	final public void setQAEventGenerators(QAItemLevelEventGenerator[] aQAEventGenerators) {
		this.maQAEventGenerators = aQAEventGenerators;
		this.miQAEvents = this.maQAEventGenerators.length;
	}

	/**
	 * Checks if is object type.
	 * 
	 * @param pObjectTypeName
	 *            the object type name
	 * 
	 * @return true, if is object type
	 */
	public boolean isObjectType(String pObjectTypeName) {
		if ((pObjectTypeName != null) && (this.mObjectType != null)
				&& this.mObjectType.equalsIgnoreCase(pObjectTypeName)) {
			return true;
		}

		return false;
	}

	/**
	 * Instantiates a new ETL port.
	 * 
	 * @param esOwningStep
	 *            the es owning step
	 * @param esSrcStep
	 *            the es src step
	 */
	public ETLPort(ETLStep esOwningStep, ETLStep esSrcStep) {
		this.mesStep = esOwningStep;
		this.mesSrcStep = esSrcStep;
	}

	// Initializes the basic attributes and checks the child nodes to relate to other steps via the HashMap.
	/**
	 * Initialize.
	 * 
	 * @param xmlConfig
	 *            the xml config
	 * 
	 * @return the int
	 * 
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
		this.mObjectType = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), ETLPort.OBJECT_TYPE_ATTRIB, null);
		this.mNode = xmlConfig;
		this.mstrName = this.getPortName();
		this.setPortClass();

		if (this.isConstant() && this.constantValue == null) {
			this.instantiateConstant();
		}

		if (this.mesStep.getQACollection().addQAForItem(this, xmlConfig)) {
			this.mbQAItemsExist = true;
		}

		//ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Creating "
		//		+ (this instanceof ETLInPort ? "IN" : "OUT") + " port " + this.mesStep.getName() + "."
		//		+ this.getPortName());
		return 0;
	}

	/**
	 * Gets the port class.
	 * 
	 * @return the port class
	 */
	public Class getPortClass() {
		return this.mDataType;
	}

	/** The is array. */
	private boolean isArray = false;

	/** The default type used. */
	private boolean defaultTypeUsed = false;

	/**
	 * Gets the code generation reference object.
	 * 
	 * @return the code generation reference object
	 */
	abstract public String getCodeGenerationReferenceObject();

	/**
	 * Sets the port class.
	 * 
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	protected void setPortClass() throws ClassNotFoundException {
		String dType = XMLHelper.getAttributeAsString(this.mNode.getAttributes(), "DATATYPE", null);
		if (dType != null) {

			int id = DataItemHelper.getDataTypeIDbyName(dType);

			if (id == -1) {
				if (dType.endsWith("[]")) {
					this.isArray = true;
					this.mDataType = java.lang.reflect.Array.newInstance(
							Class.forName(dType.substring(0, dType.length() - 2)), 0).getClass();
				} else
					this.mDataType = Class.forName(dType);
			} else {
				this.mDataType = DataItemHelper.getClassForDataType(id);
			}
		} else if (dType == null && this.isConstant()) {
			if (dType == null) {
				ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE, "Defaulting port "
						+ this.mesStep.getName() + "."
						+ XMLHelper.getAttributeAsString(this.mNode.getAttributes(), "NAME", null)
						+ " datatype to java.lang.String");
				dType = "java.lang.String";
				this.mDataType = String.class;
				this.defaultTypeUsed = true;
				((Element) this.mNode).setAttribute("DATATYPE", dType);
			}
		}
	}

	/**
	 * Checks if is array.
	 * 
	 * @return true, if is array
	 */
	final public boolean isArray() {
		return this.isArray;
	}

	/**
	 * Gets the XML config.
	 * 
	 * @return the XML config
	 */
	public Element getXMLConfig() {
		return (Element) this.mNode;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.mesStep.getName() + "." + this.mstrName + "[" + XMLHelper.getTextContent(this.getXMLConfig()) + "]";
	}

	/** The mi QA events. */
	int miQAEvents;

	/**
	 * Contains code.
	 * 
	 * @return true, if successful
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public boolean containsCode() throws KETLThreadException {
		return (this.getCode() == null ? false : true);
	}

	/** The constant. */
	private Boolean constant = null;

	/**
	 * Checks if is constant.
	 * 
	 * @return true, if is constant
	 */
	final public boolean isConstant() {

		if (this.constant == null) {
			this.constant = ETLPort.containsConstant(XMLHelper.getTextContent(this.getXMLConfig()));
		}

		return this.constant.booleanValue();
	}

	/**
	 * Contains constant.
	 * 
	 * @param content
	 *            the content
	 * 
	 * @return true, if successful
	 */
	public static boolean containsConstant(String content) {
		if (content == null)
			return false;
		else {
			content = content.trim();

			if (content.startsWith("\"") && content.endsWith("\""))
				return true;
			else
				return false;
		}
	}

	/**
	 * Gets the code.
	 * 
	 * @return the code
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public String getCode() throws KETLThreadException {
		if (this.isConstant())
			return null;

		String content = XMLHelper.getTextContent(this.getXMLConfig());

		if (content == null
				|| content.trim().length() == 0
				)
			return null;
		else if ((content.trim().startsWith(com.kni.etl.EngineConstants.VARIABLE_PARAMETER_START) && content.trim()
						.endsWith(com.kni.etl.EngineConstants.VARIABLE_PARAMETER_END)) && EngineConstants.getParametersFromText(content).length==1)			
			return null;

		return content;
	}

	/**
	 * Gets the associated in port.
	 * 
	 * @return the associated in port
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public ETLPort getAssociatedInPort() throws KETLThreadException {
		if (this.getCode() != null)
			throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
					+ " does not contain a valid mapping, make sure datatype is declared for ports containing code",
					this);

		String[] port = EngineConstants.getParametersFromText(XMLHelper.getTextContent(this.getXMLConfig()));

		if (port == null || port.length != 1)
			throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
					+ " does not contain a valid mapping, see XML: " + XMLHelper.outputXML(this.getXMLConfig()), this);

		ETLInPort p = this.mesStep.getInPort(port[0]);

		if (p == null)
			throw new KETLThreadException("Port " + this.mesStep.getName() + ", see XML: "
					+ XMLHelper.outputXML(this.getXMLConfig()) + " contains an invalid in port name " + port[0], this);

		return p;
	}

	/**
	 * Gets the port index.
	 * 
	 * @return the port index
	 */
	public int getPortIndex() {
		return this.mIndex;
	}

	/** The index. */
	private int mIndex = -1;

	/**
	 * Sets the index.
	 * 
	 * @param index
	 *            the new index
	 */
	final public void setIndex(int index) {
		this.mIndex = index;
	}

	/**
	 * Sets the data type from port.
	 * 
	 * @param in
	 *            the new data type from port
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
		(this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getName());
		this.setPortClass();
	}

	/** The constant value. */
	private Object constantValue = null;

	/**
	 * Instantiate constant.
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public void instantiateConstant() throws KETLThreadException {
		try {
			String txt = this.getXMLConfig().getTextContent().trim();
			txt = txt.substring(1, txt.length() - 1);
			Constructor con = this.getPortClass().getConstructor(new Class[] { String.class });
			this.constantValue = con.newInstance(new Object[] { txt });
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}
	}

	/**
	 * Gets the constant value.
	 * 
	 * @return the constant value
	 */
	final public Object getConstantValue() {
		return this.constantValue;
	}

	/**
	 * Use inherited data type.
	 * 
	 * @return true, if successful
	 */
	final public boolean useInheritedDataType() {
		if (this.defaultTypeUsed)
			return true;

		return false;

	}

	final public void setDataType(Class cls) {
		this.mDataType = cls;
	}
}
