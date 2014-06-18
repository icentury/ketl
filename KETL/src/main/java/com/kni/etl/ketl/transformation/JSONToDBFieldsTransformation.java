/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.transformation;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.Format;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.jayway.jsonpath.JsonPath;
import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.reader.JDBCReader;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

/**
 * The Class XMLToFieldsTransformation.
 */
public class JSONToDBFieldsTransformation extends ETLTransformation implements DBConnection {

  @Override
  protected String getVersion() {
    return "$LastChangedRevision: 491 $";
  }

  private Connection mcDBConnection;

  private String mstrTableName;

  /**
   * Gets the connection.
   * 
   * @param paramList the param list
   * 
   * @return the connection
   * @throws Exception
   */
  private Connection getConnection(int paramList) throws Exception {
    if (this.mcDBConnection != null)
      ResourcePool.releaseConnection(this.mcDBConnection);

    Properties props = JDBCItemHelper.getProperties(this.getParameterListValues(paramList));
    this.mcDBConnection =
        ResourcePool.getConnection(this.getParameterValue(paramList, DBConnection.DRIVER_ATTRIB),
            this.getParameterValue(paramList, DBConnection.URL_ATTRIB),
            this.getParameterValue(paramList, DBConnection.USER_ATTRIB),
            this.getParameterValue(paramList, DBConnection.PASSWORD_ATTRIB),
            this.getParameterValue(paramList, JDBCReader.PRESQL_ATTRIB), true, props);

    return this.mcDBConnection;
  }

  private String getJavaType(int pSQLType, int pLength, int pPrecision, int pScale) {

    return this.jdbcHelper.getJavaType(pSQLType, pLength, pPrecision, pScale);

  }

  /** The jdbc helper. */
  private JDBCItemHelper jdbcHelper;

  private void instantiateHelper(Node pXMLConfig) throws KETLThreadException {
    if (this.jdbcHelper != null)
      return;

    String hdl = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), "HANDLER", null);

    if (hdl == null)
      this.jdbcHelper = new JDBCItemHelper();
    else {
      try {
        Class<?> cl = Class.forName(hdl);
        this.jdbcHelper = (JDBCItemHelper) cl.newInstance();
      } catch (Exception e) {
        throw new KETLThreadException("HANDLER class not found", e, this);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.ETLReader#overrideOuts() Determine outs from query.
   */
  @Override
  protected void overrideOuts(ETLWorker srcWorker) throws KETLThreadException {
    super.overrideOuts(srcWorker);

    this.instantiateHelper(this.getXMLConfig());

    if (outsResolved(this.getXMLConfig())) {
      ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE,
          "Outputs resolved in another partition");
      return;
    }


    try {
      Connection mcDBConnection;
      try {
        mcDBConnection = this.getConnection(0);
      } catch (ClassNotFoundException e) {
        throw new KETLThreadException(e, this);
      }

      this.setGroup(EngineConstants.cleanseDatabaseName(this.mcDBConnection.getMetaData()
          .getDatabaseProductName()));

      this.mstrTableName =
          XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), TABLE_ATTRIB, null);
      String sql = this.getStepTemplate(this.getGroup(), "GETCOLUMNS", true);
      sql = EngineConstants.replaceParameterV2(sql, "QUERY", "select * from " + this.mstrTableName);
      PreparedStatement mStmt = mcDBConnection.prepareStatement(sql);

      mStmt.execute();

      // Log executing sql to feed result record object with single
      // object reference
      ResultSetMetaData rm = mStmt.getMetaData();

      int cols = rm.getColumnCount();

      Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), ETLStep.OUT_TAG, "*", "*");

      String channel = "DEFAULT";
      if (nl != null) {
        for (Node element : nl) {
          channel = XMLHelper.getAttributeAsString(element.getAttributes(), "CHANNEL", channel);
          this.getXMLConfig().removeChild(element);
        }
      }
      JSONObject jsonMap;
      Map<String, String> transformMap = new HashMap<String, String>();
      String mappingQuery =
          XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
              "MAPPING_QUERY_PARAMETER", null);
      // select field_name,jpath from mapping where table_name = 'abc'
      if (mappingQuery != null) {
        Statement stmt = mcDBConnection.createStatement();
        ResultSet rs = null;
        try {
          rs = stmt.executeQuery(mappingQuery);
          int colCnt = rs.getMetaData().getColumnCount();
          jsonMap = new JSONObject();
          while (rs.next()) {
            if (colCnt == 2)
              jsonMap.put(rs.getString(1), rs.getString(2));
            else if (colCnt == 3 && rs.getString(3) != null)
              transformMap.put(rs.getString(1), rs.getString(3));
          }
        } finally {
          if (rs != null)
            rs.close();
          stmt.close();
        }
      } else {
        String mappingParam =
            XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
                "MAPPING_PARAMETER", null);
        jsonMap =
            mappingParam == null ? null : (JSONObject) JSONValue.parse(this.getParameterValue(0,
                mappingParam));
      }
      for (int i = 1; i <= cols; i++) {
        Element newOut = this.getXMLConfig().getOwnerDocument().createElement(ETLStep.OUT_TAG);

        newOut.setAttribute("NAME", rm.getColumnName(i));
        String type =
            this.getJavaType(rm.getColumnType(i), JDBCReader.getColumnDisplaySize(rm, i),
                JDBCReader.getPrecision(rm, i), JDBCReader.getScale(rm, i));
        newOut.setAttribute("DATATYPE", type);
        newOut.setAttribute("CHANNEL", channel);

        if (jsonMap == null || !jsonMap.containsKey(rm.getColumnName(i)))
          newOut.setAttribute("JPATH", "$." + rm.getColumnName(i));
        else
          newOut.setAttribute("JPATH", (String) jsonMap.get(rm.getColumnName(i)));

        this.getXMLConfig().appendChild(
            this.getXMLConfig().getOwnerDocument().importNode(newOut, true));
        ResourcePool.LogMessage(
            this,
            ResourcePool.DEBUG_MESSAGE,
            "Inferring port "
                + XMLHelper.getAttributeAsString(newOut.getAttributes(), "NAME", "N/A") + " as "
                + newOut.getAttribute("DATATYPE"));
      }

      // Close open resources
      if (mStmt != null) {
        mStmt.close();
      }

      ResourcePool.releaseConnection(mcDBConnection);
      this.mcDBConnection = null;
    } catch (Exception e1) {
      throw new KETLThreadException("Problem inferring outs - " + e1.getMessage(), e1, this);
    }


  }

  @Override
  protected boolean alwaysOverrideOuts() {
    return true;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLTransform#initialize(org.w3c.dom.Node)
   */
  @Override
  protected int initialize(Node xmlConfig) throws KETLThreadException {
    int res = super.initialize(xmlConfig);

    // Pull the parameters from the list...
    // Get the attributes
    NamedNodeMap nmAttrs = xmlConfig.getAttributes();

    this.mstrTableName = XMLHelper.getAttributeAsString(nmAttrs, TABLE_ATTRIB, null);

    if (res != 0)
      return res;

    return 0;
  }



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
  public JSONToDBFieldsTransformation(Node pXMLConfig, int pPartitionID, int pPartition,
      ETLThreadManager pThreadManager) throws KETLThreadException {
    super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    try {


    } catch (Exception e) {
      throw new KETLThreadException(e, this);
    }

  }



  /** The xml src port. */
  JSONETLInPort jsonSrcPort = null;

  /**
   * The Class XMLETLInPort.
   */
  class JSONETLInPort extends ETLInPort {

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
      int res = super.initialize(xmlConfig);
      if (res != 0)
        return res;



      if (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "JSONDATA", false)) {
        if (JSONToDBFieldsTransformation.this.jsonSrcPort != null)
          throw new KETLThreadException("Only one port can be assigned as JSONData", this);
        JSONToDBFieldsTransformation.this.jsonSrcPort = this;
      }

      return 0;
    }

    /**
     * Instantiates a new XMLETL in port.
     * 
     * @param esOwningStep the es owning step
     * @param esSrcStep the es src step
     */
    public JSONETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
      super(esOwningStep, esSrcStep);
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
   */
  @Override
  protected ETLOutPort getNewOutPort(ETLStep srcStep) {
    return new JSONETLOutPort(this, srcStep);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
   */
  @Override
  protected ETLInPort getNewInPort(ETLStep srcStep) {
    return new JSONETLInPort(this, srcStep);
  }

  /** The FORMA t_ STRING. */
  public static String FORMAT_STRING = "FORMATSTRING";

  /**
   * The Class XMLETLOutPort.
   */
  class JSONETLOutPort extends ETLOutPort {
    @Override
    final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException,
        ClassNotFoundException {
      if (this.jpath == null || this.getXMLConfig().hasAttribute("DATATYPE") == false)
        (this.getXMLConfig()).setAttribute("DATATYPE", in.getPortClass().getCanonicalName());
      this.setPortClass();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLPort#getAssociatedInPort()
     */
    @Override
    public ETLPort getAssociatedInPort() throws KETLThreadException {
      return JSONToDBFieldsTransformation.this.jsonSrcPort;
    }



    /** The xpath. */
    String fmt, jpath;

    /** The formatter. */
    Format formatter;

    /** The attribute. */
    String attribute = null;

    /** The position. */
    ParsePosition position;


    /** The null IF. */
    String nullIF = null;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {

      this.nullIF = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NULLIF", null);
      this.jpath = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "JPATH", null);
      int res = super.initialize(xmlConfig);

      if (res != 0)
        return res;



      this.fmt =
          XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
              JSONToDBFieldsTransformation.FORMAT_STRING, null);



      return 0;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLOutPort#generateCode(int)
     */
    @Override
    public String generateCode(int portReferenceIndex) throws KETLThreadException {

      if (this.jpath == null || this.isConstant() || this.isUsed() == false)
        return super.generateCode(portReferenceIndex);

      // must be pure code then do some replacing

      return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this)
          + "] = ((" + this.mesStep.getClass().getCanonicalName()
          + ")this.getOwner()).getJSONValue(" + portReferenceIndex + ")";

    }

    /**
     * Instantiates a new XMLETL out port.
     * 
     * @param esOwningStep the es owning step
     * @param esSrcStep the es src step
     */
    public JSONETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
      super(esOwningStep, esSrcStep);
    }

    public Object getValue(JSONObject currentNode) {
      if (this.jpath == null)
        return null;

      return JsonPath.read(currentNode, this.jpath);
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodFooter()
   */
  @Override
  protected String getRecordExecuteMethodFooter() {
    if (this.jsonSrcPort == null)
      return super.getRecordExecuteMethodFooter();

    return " return ((" + this.getClass().getCanonicalName()
        + ")this.getOwner()).noMoreNodes()?SUCCESS:REPEAT_RECORD;}";
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodHeader()
   */
  @Override
  protected String getRecordExecuteMethodHeader() throws KETLThreadException {
    if (this.jsonSrcPort == null)
      return super.getRecordExecuteMethodHeader();

    return super.getRecordExecuteMethodHeader() + " if(((" + this.getClass().getCanonicalName()
        + ")this.getOwner()).loadJSONList(" + this.jsonSrcPort.generateReference()
        + ") == false) return SKIP_RECORD;";
  }

  private String currentJSONString = null;

  private List<JSONObject> nodeList = new ArrayList<JSONObject>();


  private JSONObject currentNode;

  /**
   * Load node list.
   * 
   * @param string the string
   * 
   * @return true, if successful
   * 
   * @throws KETLTransformException the KETL transform exception
   */
  public boolean loadJSONList(String string) throws KETLTransformException {
    try {
      if (this.currentJSONString == null || this.currentJSONString.equals(string) == false) {

        if (string == null)
          return false;

        if (string.startsWith("[")) {
          this.nodeList = (List<JSONObject>) JSONValue.parse(string);
        } else
          this.nodeList.add((JSONObject) JSONValue.parse(string));

        if (this.nodeList.size() == 0)
          return false;

        this.currentJSONString = string;
      }

      this.currentNode = this.nodeList.remove(0);

      return true;
    } catch (Exception e) {
      if (e instanceof KETLTransformException)
        throw (KETLTransformException) e;

      throw new KETLTransformException(e);
    }
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
  public Object getJSONValue(int i) throws KETLTransformException {
    JSONETLOutPort port = (JSONETLOutPort) this.mOutPorts[i];

    try {
      Object val = port.getValue(this.currentNode);

      if (val == null || (port.nullIF != null && port.nullIF.equals(val)))
        return null;

      Class<?> cl = port.getPortClass();

      if (val.getClass() == cl)
        return val;

      String result = val.toString();

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

      Constructor<?> con;
      try {
        con = cl.getConstructor(new Class[] {String.class});
      } catch (Exception e) {
        throw new KETLTransformException("No constructor found for class " + cl.getCanonicalName()
            + " that accepts a single string");
      }
      return con.newInstance(new Object[] {result});

    } catch (Exception e) {
      throw new KETLTransformException("XML parsing failed for port " + port.mstrName, e);
    }
  }



  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
   */
  @Override
  protected void close(boolean success, boolean jobFailed) {
    // TODO Auto-generated method stub

  }

  @Override
  public Connection getConnection() {
    return this.mcDBConnection;
  }



  /**
   * No more nodes.
   * 
   * @return true, if successful
   */
  public boolean noMoreNodes() {

    if (this.nodeList.size() == 0) {
      this.currentJSONString = null;
      return true;
    }
    return false;
  }
}
