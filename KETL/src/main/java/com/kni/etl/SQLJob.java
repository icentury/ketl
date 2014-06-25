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
package com.kni.etl;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/3/2002 6:26:39 PM)
 * 
 * @author: Administrator
 */
public class SQLJob extends ETLJob implements DBConnection {

  /**
   * The Constructor.
   * 
   * @throws Exception the exception
   */
  public SQLJob() throws Exception {
    super();
  }

  /** The Constant AUTOCOMMIT. */
  static final String AUTOCOMMIT = "AUTOCOMMIT"; // defaults to false

  /** The Constant MAXROWS. */
  static final String MAXROWS = "MAXROWS";

  /** The max rows. */
  int mMaxRows = 10000;

  /** The rs result set. */
  protected java.sql.ResultSet rsResultSet = null;

  /** The update count. */
  protected int iUpdateCount = -1;

  /** The mi database max statements. */
  public int miDatabaseMaxStatements = EngineConstants.MAX_STATEMENTS_PER_CONNECTION;

  /** The b auto commit. */
  protected boolean bAutoCommit = false;

  /** The SQL node. */
  Node mSQLNode;

  /**
   * Auto commit.
   * 
   * @return true, if successful
   */
  public boolean autoCommit() {
    return this.bAutoCommit;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ETLJob#setChildNodes(org.w3c.dom.Node)
   */
  @Override
  protected Node setChildNodes(Node pParentNode) {
    if (this.mSQLNode == null) {
      Node e = pParentNode.getOwnerDocument().createElement("SQL");
      Element x = pParentNode.getOwnerDocument().createElement("STATEMENT");
      x.setAttribute(SQLJob.AUTOCOMMIT, "FALSE");
      x.setAttribute(SQLJob.MAXROWS, Integer.toString(this.getMaxRows()));
      e.appendChild(x);
      x.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction(false).toString()));
      pParentNode.appendChild(e);

      return e;
    }

    Node e = pParentNode.getOwnerDocument().importNode(this.mSQLNode, true);
    pParentNode.appendChild(e);

    return e;
  }

  /**
   * Insert the method's description here. Creation date: (5/9/2002 2:28:24 PM)
   */
  @Override
  public void cleanup() {
    // If we still have a ResultSet open, we should close it...
    if (this.rsResultSet != null) {
      // NOTE (B. Sullivan, 2002.05.09): we should probably get the Statement object and close it as
      // well,
      // but if we ever open up the SQLJob to use PreparedStatements, then it gets messy...
      // We don't notice any leftovers on the database (ie, open cursors) in Oracle, so we're going
      // to
      // leave this out for now.
      try {
        this.rsResultSet.getStatement().close();
        this.rsResultSet.close();
      } catch (Exception e) {
        // Just eat the exception
      }
    }
  }

  /**
   * Insert the method's description here. Creation date: (5/9/2002 12:06:44 PM)
   * 
   * @throws Throwable the throwable
   */
  @Override
  protected void finalize() throws Throwable {
    this.cleanup();

    // It's good practice to call the superclass's finalize() method,
    // even if you know there is not one currently defined...
    super.finalize();
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 5:56:56 PM)
   * 
   * @return java.lang.String
   * @throws Exception
   */
  public java.lang.String getDatabaseDriverClass() throws Exception {
    return (String) this.getGlobalParameter(DBConnection.DRIVER_ATTRIB);
  }

  /**
   * Insert the method's description here. Creation date: (2002.06.24 2:55:30 PM)
   * 
   * @return int
   */
  public int getDatabaseMaxStatements() {
    return this.miDatabaseMaxStatements;
  }

  /**
   * Gets the max rows.
   * 
   * @return the max rows
   */
  public int getMaxRows() {
    return this.mMaxRows;
  }

  /**
   * Sets the max rows.
   * 
   * @param pMaxRows the new max rows
   */
  public void setMaxRows(int pMaxRows) {
    this.mMaxRows = pMaxRows;
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
   * 
   * @return java.lang.String
   * @throws Exception
   */
  public java.lang.String getDatabasePassword() throws Exception {
    return (String) this.getGlobalParameter(DBConnection.PASSWORD_ATTRIB);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
   * 
   * @return java.lang.String
   * @throws Exception
   */
  public java.lang.String getPreSQL() throws Exception {
    try {
      return (String) this.getGlobalParameter(DBConnection.PRESQL_ATTRIB);
    } catch (ParameterException e) {
      return null;
    }
  }

  public Properties getDatabaseProperties() throws Exception {
    ParameterList p = this.getGlobalParameterList();
    if (p == null)
      return null;
    return JDBCItemHelper.getProperties(p.getParameters());
  }


  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:00:10 PM)
   * 
   * @return java.lang.String
   * @throws Exception
   */
  public java.lang.String getDatabaseURL() throws Exception {
    return (String) this.getGlobalParameter(DBConnection.URL_ATTRIB);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:02:34 PM)
   * 
   * @return java.lang.String
   * @throws Exception
   */
  public java.lang.String getDatabaseUser() throws Exception {
    return (String) this.getGlobalParameter(DBConnection.USER_ATTRIB);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 5:47:45 PM)
   * 
   * @return java.sql.ResultSet
   */
  public java.sql.ResultSet getResultSet() {
    return this.rsResultSet;
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 4:46:12 PM)
   * 
   * @return java.lang.String
   * 
   * @throws Exception the exception
   */
  public java.lang.String getSQL() throws Exception {
    return this.resolveParameters((String) this.getAction(true));
  }

  /**
   * Resolve parameters.
   * 
   * @param pText the text
   * 
   * @return the string
   * 
   * @throws Exception the exception
   */
  public String resolveParameters(String pText) throws Exception {
    pText = getInternalConstants(pText);
    String[] strParms = EngineConstants.getParametersFromText(pText);
    if (strParms != null) {
      for (String element : strParms) {
        String parmValue = (String) this.getGlobalParameter(element);

        if (parmValue != null) {
          pText = EngineConstants.replaceParameter(pText, element, parmValue);
        } else {
          throw new Exception("Parameter " + element + " can not be found in parameter list");
        }
      }
    }
    return pText;
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:53:09 PM)
   * 
   * @return int
   */
  public int getUpdateCount() {
    return this.iUpdateCount;
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 5:56:56 PM)
   * 
   * @param newDatabaseDriverClass the new database driver class
   */
  public void setDatabaseDriverClass(java.lang.String newDatabaseDriverClass) {
    this.setGlobalParameter(DBConnection.DRIVER_ATTRIB, newDatabaseDriverClass);
  }

  /**
   * Insert the method's description here. Creation date: (2002.06.24 2:55:30 PM)
   * 
   * @param newDatabaseMaxStatements the new database max statements
   */
  public void setDatabaseMaxStatements(int newDatabaseMaxStatements) {
    this.miDatabaseMaxStatements = newDatabaseMaxStatements;
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
   * 
   * @param newDatabasePassword java.lang.String
   */
  public void setDatabasePassword(java.lang.String newDatabasePassword) {
    this.setGlobalParameter(DBConnection.PASSWORD_ATTRIB, newDatabasePassword);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:00:10 PM)
   * 
   * @param newDatabaseURL java.lang.String
   */
  public void setDatabaseURL(java.lang.String newDatabaseURL) {
    this.setGlobalParameter(DBConnection.URL_ATTRIB, newDatabaseURL);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:02:34 PM)
   * 
   * @param newDatabaseUser java.lang.String
   */
  public void setDatabaseUser(java.lang.String newDatabaseUser) {
    this.setGlobalParameter(DBConnection.USER_ATTRIB, newDatabaseUser);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 5:47:45 PM)
   * 
   * @param newResult java.sql.ResultSet
   */
  public void setResultSet(java.sql.ResultSet newResult) {
    this.rsResultSet = newResult;
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 4:46:12 PM)
   * 
   * @param newSQL java.lang.String
   * 
   * @throws Exception the exception
   */
  public void setSQL(String newSQL) throws Exception {
    this.setAction(newSQL);
  }

  /**
   * Insert the method's description here. Creation date: (5/4/2002 6:53:09 PM)
   * 
   * @param newUpdateCount int
   */
  public void setUpdateCount(int newUpdateCount) {
    this.iUpdateCount = newUpdateCount;
  }

  /**
   * Returns a String that represents the value of this object.
   * 
   * @return a string representation of the receiver
   */
  @Override
  public String toString() {
    return (String) this.getAction(true);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ETLJob#setAction(java.lang.Object)
   */
  @Override
  public void setAction(Object oAction) throws Exception {
    super.setAction(oAction);

    String sAction = (String) this.getAction(false);

    if (sAction == "" || sAction.indexOf("<SQL") == -1) {
      this.mSQLNode = null;
      return;
    }

    DocumentBuilder builder = null;
    Document xmlDOM = null;
    DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

    // Build a DOM out of the XML string...
    try {

      builder = dmfFactory.newDocumentBuilder();
      xmlDOM = builder.parse(new InputSource(new StringReader((String) this.getAction(false))));

      NodeList nl = xmlDOM.getElementsByTagName("SQL");

      if ((nl == null) || (nl.getLength() == 0)) {
        this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
        this.getStatus().setErrorMessage("Error reading job XML: no SQL specified.");

        return;
      }

      if (nl.getLength() > 1) {
        this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
        this.getStatus().setErrorMessage(
            "Error reading job XML: more than 1 SQL top node specified.");

        return;
      }

      this.mSQLNode = nl.item(0);
      this.setTimeout(XMLHelper.getAttributeAsInt(this.mSQLNode.getAttributes(), "TIMEOUT",
          Integer.MAX_VALUE));

      this.setNotificationMode(XMLHelper.getAttributeAsString(this.mSQLNode.getAttributes(),
          "EMAILSTATUS", null));

    } catch (org.xml.sax.SAXParseException e) {
      this.mSQLNode = null;
    } catch (Exception e) {
      this.getStatus().setErrorCode(ETLJobStatus.PENDING_CLOSURE_FAILED); // BRIAN: NEED TO SET UP
                                                                          // KETL JOB ERROR
      // CODES
      this.getStatus().setErrorMessage("Error reading job XML: " + e.getMessage());

      return;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.DBConnection#getConnection()
   */
  public Connection getConnection() throws SQLException, ClassNotFoundException {
    try {
      return ResourcePool.getConnection(this.getDatabaseDriverClass(), this.getDatabaseURL(),
          this.getDatabaseUser(), this.getDatabasePassword(), this.getPreSQL(), true,
          this.getDatabaseProperties());
    } catch (Exception e) {
      throw new SQLException(e.getMessage());
    }
  }

}
