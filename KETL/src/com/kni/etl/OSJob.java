/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

/**
 * Insert the type's description here. Creation date: (5/7/2002 2:27:49 PM)
 * 
 * @author: Administrator
 */
public class OSJob extends ETLJob {

    protected java.lang.String mstrWorkingDirectory = null;
    protected java.lang.Process mpProcess = null;
    protected int miExitValue = 0;

    /**
     * OSJob constructor comment.
     */
    public OSJob() throws Exception {
        super();
    }

    protected Node setChildNodes(Node pParentNode) {
        Element e = pParentNode.getOwnerDocument().createElement("OSJOB");
        e.appendChild(pParentNode.getOwnerDocument().createTextNode(this.getAction().toString()));
        pParentNode.appendChild(e);

        return e;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:32:19 PM)
     * 
     * @param strCommandLine java.lang.String
     */
    public OSJob(String strCommandLine) throws Exception {
        this(strCommandLine, null);
    }

    /**
     * Currently REMOVED from exposure, since Java is inconsistent (pre 1.4) with working directories Creation date:
     * (5/7/2002 2:34:14 PM)
     * 
     * @param strCommandLine java.lang.String
     * @param strEnvironmentVariables java.lang.String
     * @param strWorkingDirectory java.lang.String
     */
    private OSJob(String strCommandLine, String strWorkingDirectory) throws Exception {
        super();
        setCommandLine(strCommandLine);
        setWorkingDirectory(strWorkingDirectory);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:35:08 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getCommandLine() throws Exception {
        String cmd = (String) this.getAction();

        if (cmd.indexOf("<OSJOB") != -1) {
            DocumentBuilder builder = null;
            Document xmlDOM = null;
            DocumentBuilderFactory dmfFactory = DocumentBuilderFactory.newInstance();

            builder = dmfFactory.newDocumentBuilder();
            xmlDOM = builder.parse(new InputSource(new StringReader((String) this.getAction())));

            NodeList nl = xmlDOM.getElementsByTagName("OSJOB");

            if ((nl == null) || (nl.getLength() == 0)) {
                this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
                this.getStatus().setErrorMessage("Error reading job XML: no SQL specified.");

                return null;
            }

            if (nl.getLength() > 1) {
                this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
                this.getStatus().setErrorMessage("Error reading job XML: more than 1 SQL top node specified.");

                return null;
            }

            cmd = XMLHelper.getTextContent(nl.item(0));
        }

        String[] strParms = EngineConstants.getParametersFromText(cmd);

        if (strParms != null) {

            for (int i = 0; i < strParms.length; i++) {
                String parmValue = (String) this.getGlobalParameter(strParms[i]);

                if (parmValue != null) {
                    cmd = EngineConstants.replaceParameter(cmd, strParms[i], parmValue);
                }
                else {
                    throw new Exception("Parameter " + strParms[i] + " can not be found in parameter list");
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
        return miExitValue;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:45:56 PM)
     * 
     * @return java.lang.Process
     */
    public java.lang.Process getProcess() {
        return mpProcess;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:37:28 PM)
     * 
     * @return java.lang.String
     */
    public java.lang.String getWorkingDirectory() {
        return mstrWorkingDirectory;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:35:08 PM)
     * 
     * @param newCommandLine java.lang.String
     */
    public void setCommandLine(String strCommandLine) throws Exception {
        setAction(strCommandLine);
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:46:46 PM)
     * 
     * @param newExitValue int
     */
    public void setExitValue(int newExitValue) {
        miExitValue = newExitValue;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:45:56 PM)
     * 
     * @param newProcess java.lang.Process
     */
    public void setProcess(java.lang.Process newProcess) {
        mpProcess = newProcess;
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 2:37:28 PM)
     * 
     * @param newWorkingDirectory java.lang.String
     */
    public void setWorkingDirectory(java.lang.String newWorkingDirectory) {
        mstrWorkingDirectory = newWorkingDirectory;
    }

    /**
     * Returns a String that represents the value of this object.
     * 
     * @return a string representation of the receiver
     */
    public String toString() {
        return (String) this.getAction();
    }
}
