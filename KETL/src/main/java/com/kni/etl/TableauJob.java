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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.ketl.DBConnection;
import com.kni.etl.util.XMLHelper;
import com.kni.util.tableau.ServerConnector;
import com.kni.util.tableau.ServerConnector.Type;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/7/2002 2:27:49 PM)
 * 
 * @author: Administrator
 */
public class TableauJob extends ETLJob {

  private static final String SERVERURL_ATTRIB = "SERVER";
  protected java.lang.String projectName = null, objectName = null;
  protected ServerConnector.Type type = null;

  protected int miExitValue = 0;

  private boolean debug = false;
  private boolean synchronous;

  public TableauJob() throws Exception {
    super();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ETLJob#setChildNodes(org.w3c.dom.Node)
   */
  @Override
  protected Node setChildNodes(Node pParentNode) {
    String action = this.getAction(false).toString();
    Node e;
    if (action.indexOf("<TABLEAU") != -1) {
      DocumentBuilder builder = null;
      Document xmlDOM = null;
      DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

      try {
        builder = dmfFactory.newDocumentBuilder();
        xmlDOM = builder.parse(new InputSource(new StringReader(action)));
      } catch (Exception e1) {
        throw new RuntimeException(e1);
      }

      e = pParentNode.getOwnerDocument().importNode(xmlDOM.getFirstChild(), true);
    } else {
      e = pParentNode.getOwnerDocument().createElement("TABLEAU");
      e.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction(false).toString()));
    }

    pParentNode.appendChild(e);

    return e;
  }



  private void setType(String type) {
    this.type = ServerConnector.Type.valueOf(type.toLowerCase());
  }

  private void setObjectName(String name) {
    this.objectName = name;

  }

  public boolean isDebug() {
    return debug;
  }

  /**
   * Insert the method's description here. Creation date: (5/7/2002 2:35:08 PM)
   * 
   * @return java.lang.String
   * 
   * @throws Exception the exception
   */
  public void initRefresh() throws Exception {
    String cmd = (String) this.getAction(true);

    if (cmd.indexOf("<TABLEAU") != -1) {
      DocumentBuilder builder = null;
      Document xmlDOM = null;
      DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

      builder = dmfFactory.newDocumentBuilder();
      xmlDOM = builder.parse(new InputSource(new StringReader((String) this.getAction(true))));

      NodeList nl = xmlDOM.getElementsByTagName("TABLEAU");



      debug = XMLHelper.getAttributeAsBoolean(nl.item(0).getAttributes(), "DEBUG", false);
      this.setTimeout(XMLHelper.getAttributeAsInt(nl.item(0).getAttributes(), "TIMEOUT",
          Integer.MAX_VALUE));

      this.setNotificationMode(XMLHelper.getAttributeAsString(nl.item(0).getAttributes(),
          "EMAILSTATUS", null));
      this.setObjectProject(XMLHelper.getAttributeAsString(nl.item(0).getAttributes(), "PROJECT",
          null));
      this.synchronous =
          XMLHelper.getAttributeAsBoolean(nl.item(0).getAttributes(), "SYNCHRONOUS", true);

      for (Type t : ServerConnector.Type.values()) {
        this.objectName =
            XMLHelper
                .getAttributeAsString(nl.item(0).getAttributes(), t.name().toUpperCase(), null);
        if (this.objectName != null) {
          this.type = t;
          break;
        }
      }
    }

  }

  /**
   * Insert the method's description here. Creation date: (5/7/2002 2:46:46 PM)
   * 
   * @return int
   */
  public int getExitValue() {
    return this.miExitValue;
  }

  /**
   * Insert the method's description here. Creation date: (5/7/2002 2:35:08 PM)
   * 
   * @param strCommandLine the str command line
   * 
   * @throws Exception the exception
   */
  public void setObjectProject(String project) throws Exception {
    this.projectName = project;
  }

  /**
   * Returns a String that represents the value of this object.
   * 
   * @return a string representation of the receiver
   */
  @Override
  public String toString() {
    return this.getJobID();

  }

  public String getJobTriggers() throws Exception {
    String cmd = (String) this.getAction(true);

    if (cmd.indexOf("<TABLEAU") != -1) {
      DocumentBuilder builder = null;
      Document xmlDOM = null;
      DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

      builder = dmfFactory.newDocumentBuilder();
      xmlDOM = builder.parse(new InputSource(new StringReader(cmd)));

      NodeList nl = xmlDOM.getElementsByTagName("TABLEAU");

      if ((nl == null) || (nl.getLength() == 0)) {
        this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL
        // JOB ERROR CODES
        this.getStatus().setErrorMessage("Error reading job XML: no Command specified.");

        return null;
      }

      if (nl.getLength() > 1) {
        this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL
        // JOB ERROR CODES
        this.getStatus().setErrorMessage(
            "Error reading job XML: more than 1 Command top node specified.");

        return null;
      }

      return XMLHelper.getAttributeAsString(nl.item(0).getAttributes(), "JOBTRIGGERS", null);
    }
    return null;
  }

  public boolean getSynchronous() {
    return this.synchronous;
  }

  public Type getType() {
    return this.type;
  }

  public String getObjectName() {
    return this.objectName;
  }

  public String getObjectProject() {
    return this.projectName;
  }

  public String getPassword() throws Exception {
    return (String) this.getGlobalParameter(DBConnection.PASSWORD_ATTRIB);
  }

  public String getServerAddress() throws Exception {
    return (String) this.getGlobalParameter(SERVERURL_ATTRIB);

  }

  public String getUsername() throws Exception {
    return (String) this.getGlobalParameter(DBConnection.USER_ATTRIB);

  }
}
