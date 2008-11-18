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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.lookup.CachedIndexedMap;
import com.kni.etl.ketl.lookup.PersistentMap;
import com.kni.etl.stringtools.NumberFormatter;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (5/15/2002 4:49:06 PM)
 * 
 * @author: Administrator
 */
public class EngineConstants {

	private enum  DBType  { POSTGRESQL, ORACLE, MYSQL, HSQLDB, H2,TERADATA, SQLSERVER };
	
    /** The Constant SESSION_BROWSER. */
    public final static int SESSION_BROWSER = 11;
    
    /** The Constant SESSION_FIRST_CLICK_IDENTIFIER. */
    public final static int SESSION_FIRST_CLICK_IDENTIFIER = 10;
    
    /** The Constant SESSION_MAIN_SESSION_IDENTIFIER. */
    public final static int SESSION_MAIN_SESSION_IDENTIFIER = 9;
    
    /** The Constant SESSION_IP_ADDRESS. */
    public final static int SESSION_IP_ADDRESS = 12;
    
    /** The Constant SESSION_PERSISTANT_IDENTIFIER. */
    public final static int SESSION_PERSISTANT_IDENTIFIER = 13;
    
    /** The Constant KEEP_COOKIE_VARIABLE. */
    public final static int KEEP_COOKIE_VARIABLE = 19;
    
    /** The Constant OTHER_ERROR_EXIT_CODE. */
    public final static int OTHER_ERROR_EXIT_CODE = -10;
    
    /** The Constant WRONG_ARGUMENT_EXIT_CODE. */
    public final static int WRONG_ARGUMENT_EXIT_CODE = -1;
    
    /** The Constant READXML_ERROR_EXIT_CODE. */
    public final static int READXML_ERROR_EXIT_CODE = -2;
    
    /** The Constant MULTIJOB_JOB_OVERRIDE_ERROR_EXIT_CODE. */
    public final static int MULTIJOB_JOB_OVERRIDE_ERROR_EXIT_CODE = -3;
    
    /** The Constant BADLY_FORMED_ARGUMENT_EXIT_CODE. */
    public final static int BADLY_FORMED_ARGUMENT_EXIT_CODE = -4;
    
    /** The Constant INVALID_XML_EXIT_CODE. */
    public final static int INVALID_XML_EXIT_CODE = -5;
    
    /** The Constant METADATA_ERROR_EXIT_CODE. */
    public final static int METADATA_ERROR_EXIT_CODE = -6;
    
    /** The Constant ERROR_INHERITING_XML_CODE. */
    public final static int ERROR_INHERITING_XML_CODE = -7;
    
    /** The Constant ERROR_REPLACING_PARAMETER_IN_XML_CODE. */
    public final static int ERROR_REPLACING_PARAMETER_IN_XML_CODE = -8;
    
    /** The Constant ERROR_READING_JOB_XML_CODE. */
    public final static int ERROR_READING_JOB_XML_CODE = -9;
    
    /** The Constant ERROR_STARTING_STEP_XML_CODE. */
    public final static int ERROR_STARTING_STEP_XML_CODE = -11;
    
    /** The Constant SERVER_NAME_ERROR_EXIT_CODE. */
    public final static int SERVER_NAME_ERROR_EXIT_CODE = -12;

    /** from pageview *. */
    public final static int IP_ADDRESS = 1;
    
    /** The Constant IP_ADDRESS_STR. */
    public final static String IP_ADDRESS_STR = "IP_ADDRESS";
    
    /** The Constant IN_COOKIE. */
    public final static int IN_COOKIE = 2;
    
    /** The Constant IN_COOKIE_STR. */
    public final static String IN_COOKIE_STR = "IN_COOKIE";
    
    /** The Constant OUT_COOKIE. */
    public final static int OUT_COOKIE = 3;
    
    /** The Constant OUT_COOKIE_STR. */
    public final static String OUT_COOKIE_STR = "OUT_COOKIE";
    
    /** The Constant GET_REQUEST. */
    public final static int GET_REQUEST = 4;
    
    /** The Constant GET_REQUEST_STR. */
    public final static String GET_REQUEST_STR = "URL_REQUEST";
    
    /** The Constant BROWSER. */
    public final static int BROWSER = 5;
    
    /** The Constant BROWSER_STR. */
    public final static String BROWSER_STR = "USER_AGENT";
    
    /** The Constant HTML_ERROR_CODE. */
    public final static int HTML_ERROR_CODE = 6;
    
    /** The Constant HTML_ERROR_CODE_STR. */
    public final static String HTML_ERROR_CODE_STR = "HTML_ERROR_CODE";
    
    /** The Constant HIT_DATE_TIME. */
    public final static int HIT_DATE_TIME = 8;
    
    /** The Constant HIT_DATE_TIME_STR. */
    public final static String HIT_DATE_TIME_STR = "HIT_DATE_TIME";
    
    /** The Constant BYTES_SENT. */
    public final static int BYTES_SENT = 14;
    
    /** The Constant BYTES_SENT_STR. */
    public final static String BYTES_SENT_STR = "BYTES_SENT";
    
    /** The Constant SERVE_TIME. */
    public final static int SERVE_TIME = 15;
    
    /** The Constant SERVE_TIME_STR. */
    public final static String SERVE_TIME_STR = "SERVE_TIME";
    
    /** The Constant CANONICAL_PORT. */
    public final static int CANONICAL_PORT = 16;
    
    /** The Constant CANONICAL_PORT_STR. */
    public final static String CANONICAL_PORT_STR = "CANONICAL_PORT";
    
    /** The Constant REQUEST_PROTOCOL_STR. */
    public final static String REQUEST_PROTOCOL_STR = "REQUEST_PROTOCOL";
    
    /** The Constant REQUEST_PROTOCOL. */
    public final static int REQUEST_PROTOCOL = 18;
    
    /** The Constant REFERRER_URL_STR. */
    public final static String REFERRER_URL_STR = "REFERRER_URL";
    
    /** The Constant REFERRER_URL. */
    public final static int REFERRER_URL = 17;
    
    /** The Constant OTHER. */
    public final static int OTHER = 7;
    
    /** The Constant SERVER_NAME_STR. */
    public final static String SERVER_NAME_STR = "SERVER_NAME";
    
    /** The Constant SERVER_NAME. */
    public final static int SERVER_NAME = 20;
    
    /** The Constant REMOTE_USER_STR. */
    public final static String REMOTE_USER_STR = "REMOTE_USER";
    
    /** The Constant REMOTE_USER. */
    public final static int REMOTE_USER = 21;
    
    /** The Constant REQUEST_METHOD_STR. */
    public final static String REQUEST_METHOD_STR = "REQUEST_METHOD";
    
    /** The Constant REQUEST_METHOD. */
    public final static int REQUEST_METHOD = 22;
    
    /** The Constant QUERY_STRING_STR. */
    public final static String QUERY_STRING_STR = "QUERY_STRING";
    
    /** The Constant QUERY_STRING. */
    public final static int QUERY_STRING = 23;
    
    /** The Constant REQUEST_STRING_STR. */
    public final static String REQUEST_STRING_STR = "REQUEST_STRING";
    
    /** The Constant REQUEST_STRING. */
    public final static int REQUEST_STRING = 24;
    
    /** The Constant CUSTOM_FIELD_1_STR. */
    public final static String CUSTOM_FIELD_1_STR = "CUSTOMFIELD1";
    
    /** The Constant CUSTOM_FIELD_1. */
    public final static int CUSTOM_FIELD_1 = 25;
    
    /** The Constant CUSTOM_FIELD_2_STR. */
    public final static String CUSTOM_FIELD_2_STR = "CUSTOMFIELD2";
    
    /** The Constant CUSTOM_FIELD_2. */
    public final static int CUSTOM_FIELD_2 = 26;
    
    /** The Constant CUSTOM_FIELD_3_STR. */
    public final static String CUSTOM_FIELD_3_STR = "CUSTOMFIELD3";
    
    /** The Constant CUSTOM_FIELD_3. */
    public final static int CUSTOM_FIELD_3 = 27;
    
    /** The Constant OTHER_STR. */
    public final static String OTHER_STR = "";
    
    public static String PARTITION_PATH="PARTITIONPATH";
    
    /** The Constant OBJECT_TYPES. */
    private final static String[] OBJECT_TYPES = { null, EngineConstants.IP_ADDRESS_STR, EngineConstants.IN_COOKIE_STR,
            EngineConstants.OUT_COOKIE_STR, EngineConstants.GET_REQUEST_STR, EngineConstants.BROWSER_STR,
            EngineConstants.HTML_ERROR_CODE_STR, EngineConstants.OTHER_STR, EngineConstants.HIT_DATE_TIME_STR, null,
            null, null, null, null, EngineConstants.BYTES_SENT_STR, EngineConstants.SERVE_TIME_STR,
            EngineConstants.CANONICAL_PORT_STR, EngineConstants.REFERRER_URL_STR, EngineConstants.REQUEST_PROTOCOL_STR,
            null, EngineConstants.SERVER_NAME_STR, EngineConstants.REMOTE_USER_STR, EngineConstants.REQUEST_METHOD_STR,
            EngineConstants.QUERY_STRING_STR, EngineConstants.REQUEST_STRING_STR, EngineConstants.CUSTOM_FIELD_1_STR,
            EngineConstants.CUSTOM_FIELD_2_STR, EngineConstants.CUSTOM_FIELD_3_STR };

    /**
     * Resolve object name to ID.
     * 
     * @param psObjectName the ps object name
     * 
     * @return the int
     */
    public final static int resolveObjectNameToID(String psObjectName) {
        if (psObjectName == null) {
            return -1;
        }

        for (int i = 0; i < EngineConstants.OBJECT_TYPES.length; i++) {
            if ((EngineConstants.OBJECT_TYPES[i] != null)
                    && psObjectName.equalsIgnoreCase(EngineConstants.OBJECT_TYPES[i])) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Resolve object ID to name.
     * 
     * @param psObjectType the ps object type
     * 
     * @return the string
     */
    public final static String resolveObjectIDToName(int psObjectType) {
        if ((psObjectType >= 0) && (psObjectType < EngineConstants.OBJECT_TYPES.length)) {
            return EngineConstants.OBJECT_TYPES[psObjectType];
        }

        return null;
    }

    /** The Constant CONSTANTS. */
    private final static Object[][] CONSTANTS = { { "ASC", "1" }, { "DESC", "0" } };

    /**
     * Resolve value from constant.
     * 
     * @param psConstantName the ps constant name
     * @param piDefault the pi default
     * 
     * @return the int
     */
    public final static int resolveValueFromConstant(String psConstantName, int piDefault) {
        if (psConstantName == null) {
            return piDefault;
        }

        for (Object[] o : EngineConstants.CONSTANTS) {
            if ((o != null) && (o.length == 2) && (o[0] != null) && psConstantName.equalsIgnoreCase((String) o[0])) {
                if (o[1] != null) {
                    try {
                        return Integer.parseInt((String) o[1]);
                    } catch (NumberFormatException e) {
                        ResourcePool.LogException(e, null);
                    }
                }
            }
        }

        return piDefault;
    }

    /** The Constant APACHE. */
    public final static int APACHE = 1;
    
    /** The APACH e_ STR. */
    public static String APACHE_STR = "APACHE";
    
    /** The IPLANE t_ STR. */
    public static String IPLANET_STR = "IPLANET";
    
    /** The II s_ STR. */
    public static String IIS_STR = "IIS";
    
    /** The NETSCAP e_ STR. */
    public static String NETSCAPE_STR = "NETSCAPE";
    
    /** The Constant NETSCAPE. */
    public final static int NETSCAPE = 2;
    
    /** The Constant MAX_WEBSERVERS. */
    public final static int MAX_WEBSERVERS = 3;

    /**
     * Resolve web server name to ID.
     * 
     * @param psWebServerName the ps web server name
     * 
     * @return the int
     */
    public final static int resolveWebServerNameToID(String psWebServerName) {
        if (psWebServerName == null) {
            return -1;
        }
        else if (psWebServerName.equalsIgnoreCase(EngineConstants.APACHE_STR)
                || psWebServerName.equalsIgnoreCase(EngineConstants.IIS_STR)) {
            return EngineConstants.APACHE;
        }
        else if (psWebServerName.equalsIgnoreCase(EngineConstants.IPLANET_STR)
                || psWebServerName.equalsIgnoreCase(EngineConstants.NETSCAPE_STR)) {
            return EngineConstants.NETSCAPE;
        }
        else {
            return EngineConstants.APACHE;
        }
    }

    /** database field lengths *. */
    public final static int MAX_BROWSER_LENGTH = 255;
    
    /** The Constant MAX_REQUEST_LENGTH. */
    public final static int MAX_REQUEST_LENGTH = 2000;
    
    /** The Constant MAX_REFERRER_LENGTH. */
    public final static int MAX_REFERRER_LENGTH = 1000;
    
    /** The Constant MAX_MAIN_SESSION_IDENTIFIER_LENGTH. */
    public final static int MAX_MAIN_SESSION_IDENTIFIER_LENGTH = 100;
    
    /** The Constant MAX_FIRST_CLICK_SESSION_IDENTIFIER_LENGTH. */
    public final static int MAX_FIRST_CLICK_SESSION_IDENTIFIER_LENGTH = 100;
    
    /** The Constant MAX_START_PERSISTANT_IDENTIFIER_LENGTH. */
    public final static int MAX_START_PERSISTANT_IDENTIFIER_LENGTH = 100;
    
    /** The Constant MAX_PERSISTANT_IDENTIFIER_LENGTH. */
    public final static int MAX_PERSISTANT_IDENTIFIER_LENGTH = 100;
    
    /** The Constant MAX_KEEP_VARIABLE_LENGTH. */
    public final static int MAX_KEEP_VARIABLE_LENGTH = 255;

    /** string constants *. */
    public final static String INVALID_MAIN_SESSION_IDENTIFIER_STRING = "-invalid-";

    /** other default database values *. */
    public final static int MAX_STATEMENTS_PER_CONNECTION = 400;
    
    /** The MA x_ ERRO r_ MESSAG e_ LENGTH. */
    public static int MAX_ERROR_MESSAGE_LENGTH = 800;
    
    /** The VARIABL e_ PARAMETE r_ START. */
    public static String VARIABLE_PARAMETER_START = "!@#";
    
    /** The VARIABL e_ PARAMETE r_ END. */
    public static String VARIABLE_PARAMETER_END = "#@!";
    
    /** The Constant PARAMETER_JOB_EXECUTION_ID. */
    public static final String[] PARAMETER_JOB_EXECUTION_ID = { "DM_LOAD_ID", "JOB_EXEC_ID" };
    
    /** The Constant PARAMETER_LOAD_ID. */
    public static final String[] PARAMETER_LOAD_ID = { "LOAD_ID" };
    
    /** The Constant PARAMETER_DATE. */
    public static final String[] PARAMETER_DATE = { "DATE", "TIMESTAMP", "TIME" };
    
    /** The Constant PARAMETER_DATE_FORMAT. */
    public static final String[] PARAMETER_DATE_FORMAT = { "dd-MM-yyyy", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss" };
    
    /** The Constant JOB_PERSISTENCE. */
    public static final int JOB_PERSISTENCE = 0;
    
    /** The Constant LOAD_PERSISTENCE. */
    public static final int LOAD_PERSISTENCE = 1;
    
    /** The Constant STATIC_PERSISTENCE. */
    public static final int STATIC_PERSISTENCE = 2;

	public static final String DEFAULT_POOL = "Default";
    
    /** The DEFAULTCACHESIZE. */
    private static String DEFAULTCACHESIZE = "64kb";
    
    /** The LOOKUPCLASS. */
    private static String LOOKUPCLASS = null;
    
    /** The PARAMETE r_ LIST. */
    public static String PARAMETER_LIST = "PARAMETER_LIST";
    
    /** The PARAMETER. */
    public static String PARAMETER = "PARAMETER";
    
    /** The BA d_ RECOR d_ PATH. */
    public static String BAD_RECORD_PATH = "log";
    
    /** The globals. */
    private static Element globals = null;

	public static String CHECKPOINT_PATH = "checkpoint";

	public static String MONITORPATH = "monitor";
    
    /** The CACH e_ PATH. */
    public static String CACHE_PATH = ".";
    
    /** The VERSION. */
    private static String VERSION;
    
    /** The CACHEMEMRATIO. */
    private static double CACHEMEMRATIO = 0.5;

    /** The system XML. this must occur as the last parameter */
    private static Document zmSystemXML = EngineConstants._getSystemXML();

    /**
     * EngineConstants constructor comment.
     */
    public EngineConstants() {
        super();
    }

    

    /**
     * _get system XML.
     * 
     * @return the document
     */
    private static synchronized Document _getSystemXML() {

        Document doc = null;

        try {
            // get system xml
            doc = XMLHelper.readXMLFromFile(Metadata.getKETLPath() + File.separator + Metadata.SYSTEM_FILE);
            // get all plugins

            EngineConstants.globals = (Element) XMLHelper.findElementByName(doc, "GLOBAL", null, null);
            if (EngineConstants.globals != null) {
                Element e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME",
                        "PARAMETERSTART");
                if (e != null) {
                    EngineConstants.VARIABLE_PARAMETER_START = XMLHelper.getTextContent(e);
                }

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "VERSION");
                if (e != null) {
                    EngineConstants.VERSION = XMLHelper.getTextContent(e);
                    // Respectfully do not modify the following line of code without prior written permission from
                    // Kinetic Networks Inc.
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "KETL Version "
                            + EngineConstants.VERSION + ", ©" + Calendar.getInstance().get(Calendar.YEAR)
                            + " Kinetic Networks Inc.");
                    // End of section
                }

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "CACHEMEMRATIO");
                if (e != null) {
                    EngineConstants.CACHEMEMRATIO = Double.parseDouble(XMLHelper.getTextContent(e));
                }

                e = (Element) XMLHelper
                        .getElementByName(EngineConstants.globals, "OPTION", "NAME", "INMEMORYCACHESIZE");
                if (e != null) {
                    String tmp = XMLHelper.getTextContent(e);

                    if (tmp == null)
                        tmp = "64k";

                    try {
                        NumberFormatter.convertToBytes(tmp);
                    } catch (Exception e2) {
                        ResourcePool.logError("Default in memory cache size invalid: " + tmp + ", defaulting to 64k");
                        tmp = "64k";
                    }
                    EngineConstants.DEFAULTCACHESIZE = tmp;
                }
                else
                    EngineConstants.DEFAULTCACHESIZE = "64k";

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "LOOKUPCLASS");

                boolean lookForAlternative = false;
                if (e != null) {
                    String tmp = XMLHelper.getTextContent(e);
                    try {
                        Class.forName(tmp);
                        EngineConstants.LOOKUPCLASS = XMLHelper.getTextContent(e);
                    } catch (Throwable e1) {
                        lookForAlternative = true;
                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "LOOKUPCLASS "
                                + tmp + " could not be found, looking for alternative");
                    }

                }

                if (EngineConstants.LOOKUPCLASS == null) {
                    // try for perst
                    String[][] lookupsOptions = {
                            { "com.kni.etl.ketl.lookup.RawPerstIndexedMap", "org.garret.perst.Persistent" },
                            { "com.kni.etl.ketl.lookup.HSQLDBIndexedMap", "org.hsqldb.Database" } };
                    for (String[] element : lookupsOptions) {
                        try {
                            Class.forName(element[0]);
                            Class.forName(element[1]);

                            EngineConstants.LOOKUPCLASS = element[0];

                            if (lookForAlternative)
                                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                        "LOOKUPCLASS defaulted to " + EngineConstants.LOOKUPCLASS);
                            break;
                        } catch (Exception e1) {

                        }
                    }
                    if (EngineConstants.LOOKUPCLASS == null) {
                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
                                "No cache engine found");
                    }

                }

                if (EngineConstants.LOOKUPCLASS != null && lookForAlternative == false)
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "Cache engine "
                            + EngineConstants.LOOKUPCLASS);

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "PARAMETEREND");
                if (e != null) {
                    XMLHelper.getTextContent(e);
                    EngineConstants.VARIABLE_PARAMETER_END = XMLHelper.getTextContent(e);
                }

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME",
                        "MAXERRORMESSAGELENGTH");
                if (e != null) {
                    try {
                        EngineConstants.MAX_ERROR_MESSAGE_LENGTH = Integer.parseInt(XMLHelper.getTextContent(e));
                    } catch (Exception e1) {

                    }
                }

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "BADRECORDPATH");
                if (e != null) {
                    try {
                        EngineConstants.BAD_RECORD_PATH = XMLHelper.getTextContent(e);                        
                    } catch (Exception e1) {
                    }
                }
                
                
                checkPath(EngineConstants.BAD_RECORD_PATH,"bad record path");
                
                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "CHECKPOINTPATH");
                if (e != null) {
                    try {
                        EngineConstants.CHECKPOINT_PATH = XMLHelper.getTextContent(e);
                        
                    } catch (Exception e1) {

                    }
                }
                
                checkPath(EngineConstants.CHECKPOINT_PATH,"checkpoint path");
                
                
                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "MONITORPATH");
                if (e != null) {
                    try {
                        EngineConstants.MONITORPATH = XMLHelper.getTextContent(e);                        
                    } catch (Exception e1) {
                    }
                }
               
                checkPath(EngineConstants.MONITORPATH,"monitor path");

                e = (Element) XMLHelper.getElementByName(EngineConstants.globals, "OPTION", "NAME", "CACHEPATH");
                if (e != null) {
                    try {
                        EngineConstants.CACHE_PATH = XMLHelper.getTextContent(e);                        
                    } catch (Exception e1) {

                    }
                }
                
                checkPath(EngineConstants.CACHE_PATH,"cache path");
            }

            File dir = new File(Metadata.getKETLPath() + File.separator + "xml" + File.separator + "plugins");

            if (dir.isDirectory()) {
                String[] children = dir.list();
                if (children == null) {
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "0 plugins found");
                }
                else {
                    for (String element : children) {
                        // Get filename of file or directory
                        if (element.endsWith(".xml") && new File(dir, element).isFile()) {
                            try {
                                Document pluginDoc = XMLHelper.readXMLFromFile(dir.getAbsolutePath() + File.separator
                                        + element);

                                Node[] node = XMLHelper.findElementsByName(pluginDoc, "STEP", null, null);
                                for (Node element0 : node) {
                                    String pluginName = XMLHelper.getAttributeAsString(element0.getAttributes(),
                                            "CLASS", null);
                                    if (pluginName == null) {
                                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                                                "Plugin in file " + element + " does not have a name.");
                                    }
                                    else {
                                        try {
                                            Class.forName(pluginName);
                                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                                                    "Plugin " + pluginName + " enabled.");
                                            doc.getFirstChild().appendChild(doc.importNode(element0, true));
                                        } catch (Exception e) {
                                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                                                    "Plugin " + pluginName + " failed to initialize: " + e.toString());

                                        } catch (Throwable e) {
                                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                                                    "Plugin " + pluginName + " failed to initialize: " + e.toString());

                                        }
                                    }
                                }
                            } catch (Exception e) {
                                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE, e
                                        .getMessage());
                            }
                        }

                    }
                }
            }
            else if (dir.exists()) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                        "Plugins directory is not a directory, any installed plugins will not be enabled");
            }

        } catch (Exception e) {
            ResourcePool.LogMessage("System file not found or readable, expected location " + Metadata.getKETLPath()
                    + File.separator + Metadata.SYSTEM_FILE);

        }

        return doc;

    }

	private static void checkPath(String path, String name) {
		File f = new File(path);
		if (f.exists() == false) {
		    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
		            "Creating " + name + " directory " + f.getAbsolutePath());
		    f.mkdir();
		}
		else if (f.exists() && f.isDirectory() == false) {
		    System.err
		            .println("Cannot initialize " + name + " directory, as it is currently a file and not a directory: "
		                    + f.getAbsolutePath());
		    ResourcePool.logError("Please move this file or rename it: " + f.getAbsolutePath());
		}
	}

    /**
     * Gets the system XML.
     * 
     * @return the system XML
     */
    public static synchronized Document getSystemXML() {
        if (EngineConstants.zmSystemXML == null) {
            EngineConstants.zmSystemXML = EngineConstants._getSystemXML();
        }

        return EngineConstants.zmSystemXML;
    }

    /**
     * Replace parameter.
     * 
     * @param strAction the str action
     * @param pParameterToLookFor the parameter to look for
     * @param pNewValue the new value
     * 
     * @return the string
     */
    public static String replaceParameter(String strAction, String pParameterToLookFor, String pNewValue) {
        int loadIDPos = 0;

        while (loadIDPos != -1) {
            loadIDPos = strAction.indexOf(EngineConstants.VARIABLE_PARAMETER_START + pParameterToLookFor
                    + EngineConstants.VARIABLE_PARAMETER_END);

            if (loadIDPos != -1) {
                strAction = strAction.substring(0, loadIDPos)
                        + pNewValue
                        + strAction.substring(loadIDPos + EngineConstants.VARIABLE_PARAMETER_START.length()
                                + pParameterToLookFor.length() + EngineConstants.VARIABLE_PARAMETER_END.length());
            }
        }

        return strAction;
    }

    /**
     * Gets the instance of persistant map.
     * 
     * @param className the class name
     * @param pName the name
     * @param pSize the size
     * @param pPersistanceID the persistance ID
     * @param pCacheDir the cache dir
     * @param pKeyTypes the key types
     * @param pValueTypes the value types
     * @param pValueFields the value fields
     * @param pPurgeCache the purge cache
     * 
     * @return the instance of persistant map
     * 
     * @throws Throwable the throwable
     */
    final public static PersistentMap getInstanceOfPersistantMap(String className, String pName, int pSize,
            Integer pPersistanceID, String pCacheDir, Class[] pKeyTypes, Class[] pValueTypes, String[] pValueFields,
            boolean pPurgeCache) throws Throwable {
        Class cl = Class.forName(className);

        Constructor con = cl.getConstructor(new Class[] { String.class, int.class, Integer.class, String.class,
                Class[].class, Class[].class, String[].class, boolean.class });
        PersistentMap pm;

        try {
            pm = (PersistentMap) con.newInstance(new Object[] { pName, pSize, pPersistanceID, pCacheDir, pKeyTypes,
                    pValueTypes, pValueFields, pPurgeCache });
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
        if (EngineConstants.getDefaultCacheSize().equals("0") || pSize <= 0) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE,
                    "Creating direct lookup, no level 1 in memory cache");
            return pm;
        }
        else {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.DEBUG_MESSAGE,
                    "Creating lookup with associated level 1 in memory cache");
            return new CachedIndexedMap(pm);
        }

    }

    /**
     * Replace parameter v2.
     * 
     * @param strAction the str action
     * @param pParameterToLookFor the parameter to look for
     * @param pNewValue the new value
     * 
     * @return the string
     */
    public static String replaceParameterV2(String strAction, String pParameterToLookFor, String pNewValue) {
        int loadIDPos = 0;

        String st = "${";
        String ed = "}";
        while (loadIDPos != -1) {
            loadIDPos = strAction.indexOf(st + pParameterToLookFor + ed);

            if (loadIDPos != -1) {
                strAction = strAction.substring(0, loadIDPos) + pNewValue
                        + strAction.substring(loadIDPos + st.length() + pParameterToLookFor.length() + ed.length());
            }
        }

        return strAction;
    }

    /**
     * Replace parameter.
     * 
     * @param strAction the str action
     * @param pParameterToLookFor the parameter to look for
     * @param pNewValueFormat the new value format
     * @param pNewValue the new value
     * @param defaultValue the default value
     * 
     * @return the string
     */
    public static String replaceParameter(String strAction, String pParameterToLookFor, String[] pNewValueFormat,
            String[] pNewValue, String defaultValue) {
        int loadIDPos = 0;

        if ((pNewValueFormat == null) || (pNewValueFormat.length == 0)) {
            return EngineConstants.replaceParameter(strAction, pParameterToLookFor, defaultValue);
        }

        if ((pNewValue == null) || (pNewValueFormat.length != pNewValue.length)) {
            ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                    "Parameter formats and parameters value mismatch, parameter parsing");

            return strAction;
        }

        for (int i = 0; i < pNewValue.length; i++) {
            while (loadIDPos != -1) {
                loadIDPos = strAction.indexOf(EngineConstants.VARIABLE_PARAMETER_START + pParameterToLookFor + "("
                        + pNewValueFormat[i] + ")" + EngineConstants.VARIABLE_PARAMETER_END);

                if (loadIDPos != -1) {
                    String value;

                    if ((pNewValue == null) || (i > (pNewValue.length - 1)) || (pNewValue[i].length() == 0)) {
                        value = defaultValue;
                    }
                    else {
                        value = pNewValue[i];
                    }

                    strAction = strAction.substring(0, loadIDPos)
                            + value
                            + strAction.substring(loadIDPos + EngineConstants.VARIABLE_PARAMETER_START.length()
                                    + pParameterToLookFor.length() + 1 + pNewValueFormat[i].length() + 1
                                    + EngineConstants.VARIABLE_PARAMETER_END.length());
                }
            }
        }

        return strAction;
    }

    /**
     * Gets the parameter parameters.
     * 
     * @param strAction the str action
     * @param pParameterToLookFor the parameter to look for
     * 
     * @return the parameter parameters
     */
    public static String[] getParameterParameters(String strAction, String pParameterToLookFor) {
        int loadIDPos = 0;

        ArrayList res = new ArrayList();

        while (loadIDPos != -1) {
            loadIDPos = strAction.indexOf(EngineConstants.VARIABLE_PARAMETER_START + pParameterToLookFor + "(",
                    loadIDPos);

            if (loadIDPos == -1) {
                continue;
            }

            loadIDPos = loadIDPos + EngineConstants.VARIABLE_PARAMETER_START.length() + pParameterToLookFor.length()
                    + 1;

            int paramEnd = strAction.indexOf(")" + EngineConstants.VARIABLE_PARAMETER_END, loadIDPos);

            if (paramEnd == -1) {
                ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                        "End of parameter not found");

                return null;
            }

            if (loadIDPos <= paramEnd) {
                res.add(strAction.substring(loadIDPos, paramEnd));
            }
        }

        if (res.size() == 0) {
            return null;
        }

        String[] strRes = new String[res.size()];

        res.toArray(strRes);

        return strRes;
    }

    /**
     * Gets the parameters from text.
     * 
     * @param strText the str text
     * 
     * @return list of parameters
     * 
     * @description Return list of parameters in text
     */
    public static String[] getParametersFromText(String strText) {
        int paramPos = 0;
        ArrayList tmpList = new ArrayList();

        if (strText == null)
            return new String[0];

        int lastParamPos = -2;
        boolean paramExists = true;
        while (paramExists) {
            paramPos = strText.indexOf(EngineConstants.VARIABLE_PARAMETER_START, paramPos);

            if (paramPos == -1) {
                paramExists = false;
                continue;
            }

            int paramEndPos = strText.indexOf(EngineConstants.VARIABLE_PARAMETER_END, paramPos);

            if (paramEndPos == -1) {
                if (paramPos == lastParamPos)
                    paramExists = false;
                else
                    lastParamPos = paramPos;

                continue;
            }

            String paramName = strText.substring(paramPos + EngineConstants.VARIABLE_PARAMETER_START.length(),
                    paramEndPos);

            paramPos++;
            if (tmpList.contains(paramName) == false)
                tmpList.add(paramName);
        }

        String[] tmpRes = null;

        tmpRes = new String[tmpList.size()];
        tmpList.toArray(tmpRes);

        return tmpRes;
    }

    /**
     * Gets the version.
     * 
     * @return the version
     */
    public static String getVersion() {
        if (EngineConstants.VERSION == null)
            return "N/A";

        return EngineConstants.VERSION;
    }

    /**
     * Gets the cache memory ratio.
     * 
     * @return the cache memory ratio
     */
    public static double getCacheMemoryRatio() {
        return EngineConstants.CACHEMEMRATIO;
    }

    /**
     * Gets the default lookup class.
     * 
     * @return the default lookup class
     */
    public static String getDefaultLookupClass() {
        return EngineConstants.LOOKUPCLASS;
    }

    /**
     * Gets the default cache size.
     * 
     * @return the default cache size
     */
    public static String getDefaultCacheSize() {
        return EngineConstants.DEFAULTCACHESIZE;
    }

    /* determines the database type from the product name, as not all drivers return the same product name */
	public static String cleanseDatabaseName(String name) throws Exception {
		if(name == null)
			throw new Exception("Could not determine database type from name, name is null");
		
		for(DBType dbType:DBType.values()){
			
			if(name.toUpperCase().contains(dbType.name()))
				return dbType.name();
		}
		
		throw new Exception("Could not determine database type from name: " + name);	
	}
}
