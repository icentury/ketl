/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

/**
 * Insert the type's description here. Creation date: (3/5/2002 3:41:34 PM)
 * 
 * @author: Administrator
 */
public abstract class ETLPort {

    protected boolean mbQAItemsExist = false;

    public static final String NAME_ATTRIB = "NAME";
    public static final String OBJECT_TYPE_ATTRIB = "OBJECTTYPE";
    public String mstrName;
    public String mObjectType;
    private Class mDataType;
    private boolean mUsed = false;
    Node mNode;
    public ETLStep mesStep, mesSrcStep;
    QAItemLevelEventGenerator[] maQAEventGenerators = null;

    public String generateCode(int portReferenceIndex) throws KETLThreadException {
        throw new KETLThreadException("generateCode needs to be overridden for ETLWriter based steps", this);
    }

    abstract public String getPortName() throws KETLThreadException;

    public void used(boolean arg0) throws KETLThreadException {
        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Port: " + this.getPortName()
                + (arg0 ? " enabled" : " disabled"));
        this.mUsed = arg0;
    }

    public boolean isUsed() {
        return this.mUsed;
    }

    final public void setQAEventGenerators(QAItemLevelEventGenerator[] aQAEventGenerators) {
        this.maQAEventGenerators = aQAEventGenerators;
        miQAEvents = this.maQAEventGenerators.length;
    }

    public boolean isObjectType(String pObjectTypeName) {
        if ((pObjectTypeName != null) && (this.mObjectType != null)
                && this.mObjectType.equalsIgnoreCase(pObjectTypeName)) {
            return true;
        }

        return false;
    }

    public ETLPort(ETLStep esOwningStep, ETLStep esSrcStep) {
        mesStep = esOwningStep;
        mesSrcStep = esSrcStep;
    }

    // Initializes the basic attributes and checks the child nodes to relate to other steps via the HashMap.
    public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
        mObjectType = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), OBJECT_TYPE_ATTRIB, null);
        mNode = xmlConfig;
        mstrName = this.getPortName();
        this.setPortClass();

        if (this.isConstant() && this.constantValue == null) {
            this.instantiateConstant();
        }

        if (this.mesStep.getQACollection().addQAForItem(this, xmlConfig)) {
            this.mbQAItemsExist  = true;
        }
        
        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Creating "
                + (this instanceof ETLInPort ? "IN" : "OUT") + " port " + this.mesStep.getName() + "."
                + this.getPortName());
        return 0;
    }

    public Class getPortClass() {
        return this.mDataType;
    }

    private boolean isArray = false;
    private boolean defaultTypeUsed = false;

    abstract public String getCodeGenerationReferenceObject();

    protected void setPortClass() throws ClassNotFoundException {
        String dType = XMLHelper.getAttributeAsString(mNode.getAttributes(), "DATATYPE", null);
        if (dType != null) {

            int id = DataItemHelper.getDataTypeIDbyName(dType);

            if (id == -1) {
                if (dType.endsWith("[]")) {
                    isArray = true;
                    this.mDataType = java.lang.reflect.Array.newInstance(
                            Class.forName(dType.substring(0, dType.length() - 2)), 0).getClass();
                }
                else
                    this.mDataType = Class.forName(dType);
            }
            else {
                this.mDataType = DataItemHelper.getClassForDataType(id);
            }
        }
        else if (dType == null && this.isConstant()) {
            if (dType == null) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE, "Defaulting port "
                        + this.mesStep.getName() + "."
                        + XMLHelper.getAttributeAsString(mNode.getAttributes(), "NAME", null)
                        + " datatype to java.lang.String");
                dType = "java.lang.String";
                this.mDataType = String.class;
                defaultTypeUsed = true;
                ((Element) mNode).setAttribute("DATATYPE", dType);
            }
        }
    }

    final public boolean isArray() {
        return this.isArray;
    }

    public Element getXMLConfig() {
        return (Element) this.mNode;
    }

    public String toString() {
        return mesStep.getName() + "." + mstrName + "[" + XMLHelper.getTextContent(getXMLConfig()) + "]";
    }

    int miQAEvents;

    public boolean containsCode() throws KETLThreadException {
        return (this.getCode() == null ? false : true);
    }

    private Boolean constant = null;

    final public boolean isConstant() {

        if (constant == null) {
            constant = containsConstant(XMLHelper.getTextContent(this.getXMLConfig()));
        }

        return constant.booleanValue();
    }

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

    public String getCode() throws KETLThreadException {
        if (isConstant())
            return null;

        String content = XMLHelper.getTextContent(this.getXMLConfig());

        if (content == null || content.trim().length() == 0
                || (content.trim().startsWith(com.kni.etl.EngineConstants.VARIABLE_PARAMETER_START)
                && content.trim().endsWith(com.kni.etl.EngineConstants.VARIABLE_PARAMETER_END)))
            return null;

        return content;
    }

    public ETLPort getAssociatedInPort() throws KETLThreadException {
        if (getCode() != null)
            throw new KETLThreadException("Port " + this.mesStep.getName() + "." + this.getPortName()
                    + " does not contain a valid mapping, make sure datatype is declared for ports containing code", this);

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

    public int getPortIndex() {
        return mIndex;
    }

    private int mIndex = -1;

    final public void setIndex(int index) {
        mIndex = index;
    }

    public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
        ((Element) this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
        this.setPortClass();
    }

    private Object constantValue = null;

    public void instantiateConstant() throws KETLThreadException {
        try {
            String txt = this.getXMLConfig().getTextContent().trim();
            txt = txt.substring(1, txt.length() - 1);
            Constructor con = this.getPortClass().getConstructor(new Class[] { String.class });
            constantValue = con.newInstance(new Object[] { txt });
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }
    }

    final public Object getConstantValue() {
        return this.constantValue;
    }

    final public boolean useInheritedDataType() {
        if (this.defaultTypeUsed)
            return true;

        return false;

    }
}
