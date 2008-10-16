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
package com.kni.etl;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/7/2002 2:27:49 PM)
 * 
 * @author: Administrator
 */
public class OSJob extends ETLJob {

    /** The mstr working directory. */
    protected java.lang.String mstrWorkingDirectory = null;
    
    /** The mp process. */
    protected java.lang.Process mpProcess = null;
    
    /** The mi exit value. */
    protected int miExitValue = 0;

	private boolean debug = false;

    /**
     * OSJob constructor comment.
     * 
     * @throws Exception the exception
     */
    public OSJob() throws Exception {
        super();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ETLJob#setChildNodes(org.w3c.dom.Node)
     */
    @Override
    protected Node setChildNodes(Node pParentNode) {
        Element e = pParentNode.getOwnerDocument().createElement("OSJOB");
        e.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction(false).toString()));
        pParentNode.appendChild(e);

        return e;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:32:19 PM)
     * 
     * @param strCommandLine java.lang.String
     * 
     * @throws Exception the exception
     */
    public OSJob(String strCommandLine) throws Exception {
        this(strCommandLine, null);
    }

    /**
     * Currently REMOVED from exposure, since Java is inconsistent (pre 1.4) with working directories Creation date:
     * (5/7/2002 2:34:14 PM)
     * 
     * @param strCommandLine java.lang.String
     * @param strWorkingDirectory java.lang.String
     * 
     * @throws Exception the exception
     */
    private OSJob(String strCommandLine, String strWorkingDirectory) throws Exception {
        super();
        this.setCommandLine(strCommandLine);
        this.setWorkingDirectory(strWorkingDirectory);
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
    public java.lang.String getCommandLine() throws Exception {
        String cmd = (String) this.getAction(true);

        if (cmd.indexOf("<OSJOB") != -1) {
            DocumentBuilder builder = null;
            Document xmlDOM = null;
            DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

            builder = dmfFactory.newDocumentBuilder();
            xmlDOM = builder.parse(new InputSource(new StringReader((String) this.getAction(true))));
            
            NodeList nl = xmlDOM.getElementsByTagName("OSJOB");
            
            
            if ((nl == null) || (nl.getLength() == 0)) {
                this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
                this.getStatus().setErrorMessage("Error reading job XML: no Command specified.");

                return null;
            }

            if (nl.getLength() > 1) {
                this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
                this.getStatus().setErrorMessage("Error reading job XML: more than 1 Command top node specified.");

                return null;
            }
            
            debug  = XMLHelper.getAttributeAsBoolean(nl.item(0).getAttributes(), "DEBUG", false);
            this.setTimeout(XMLHelper.getAttributeAsInt(nl.item(0).getAttributes(),"TIMEOUT",Integer.MAX_VALUE));

            this.setNotificationMode(XMLHelper.getAttributeAsString(nl.item(0).getAttributes(),"EMAILSTATUS",null));
            if ((cmd = ETLJobExecutor.getExternalSourceCode(nl.item(0)))==null)
                cmd = XMLHelper.getTextContent(nl.item(0));            
        }

        String[] strParms = EngineConstants.getParametersFromText(cmd);

        if (strParms != null) {

            for (String element : strParms) {
                String parmValue = (String) this.getGlobalParameter(element);

                if (parmValue != null) {
                    cmd = EngineConstants.replaceParameter(cmd, element, parmValue);
                }
                else {
                    throw new Exception("Parameter " + element + " can not be found in parameter list");
                }
            }
        }

        return cmd;
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
     * Insert the method's description here. Creation date: (5/7/2002 2:45:56 PM)
     * 
     * @return java.lang.Process
     */
    public java.lang.Process getProcess() {
        return this.mpProcess;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:37:28 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getWorkingDirectory() {
        return this.mstrWorkingDirectory;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:35:08 PM)
     * 
     * @param strCommandLine the str command line
     * 
     * @throws Exception the exception
     */
    public void setCommandLine(String strCommandLine) throws Exception {
        this.setAction(strCommandLine);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:46:46 PM)
     * 
     * @param newExitValue int
     */
    public void setExitValue(int newExitValue) {
        this.miExitValue = newExitValue;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:45:56 PM)
     * 
     * @param newProcess java.lang.Process
     */
    public void setProcess(java.lang.Process newProcess) {
        this.mpProcess = newProcess;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:37:28 PM)
     * 
     * @param newWorkingDirectory java.lang.String
     */
    public void setWorkingDirectory(java.lang.String newWorkingDirectory) {
        this.mstrWorkingDirectory = newWorkingDirectory;
    }

    /**
     * Returns a String that represents the value of this object.
     * 
     * @return a string representation of the receiver
     */
    @Override
    public String toString() {
        try {
			return this.getCommandLine();
		} catch (Exception e) {
			return this.getJobID();
		}
    }
}
