/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

/**
 * Insert the type's description here. Creation date: (5/15/2002 4:49:06 PM)
 * 
 * @author: Administrator
 */
public class EngineConstants {

    public final static int SESSION_BROWSER = 11;
    public final static int SESSION_FIRST_CLICK_IDENTIFIER = 10;
    public final static int SESSION_MAIN_SESSION_IDENTIFIER = 9;
    public final static int SESSION_IP_ADDRESS = 12;
    public final static int SESSION_PERSISTANT_IDENTIFIER = 13;
    public final static int KEEP_COOKIE_VARIABLE = 19;
    public final static int OTHER_ERROR_EXIT_CODE = -10;
    public final static int WRONG_ARGUMENT_EXIT_CODE = -1;
    public final static int READXML_ERROR_EXIT_CODE = -2;
    public final static int MULTIJOB_JOB_OVERRIDE_ERROR_EXIT_CODE = -3;
    public final static int BADLY_FORMED_ARGUMENT_EXIT_CODE = -4;
    public final static int INVALID_XML_EXIT_CODE = -5;
    public final static int METADATA_ERROR_EXIT_CODE = -6;
    public final static int ERROR_INHERITING_XML_CODE = -7;
    public final static int ERROR_REPLACING_PARAMETER_IN_XML_CODE = -8;
    public final static int ERROR_READING_JOB_XML_CODE = -9;
    public final static int ERROR_STARTING_STEP_XML_CODE = -11;
    public final static int SERVER_NAME_ERROR_EXIT_CODE = -12;

    /** from pageview * */
    public final static int IP_ADDRESS = 1;
    public final static String IP_ADDRESS_STR = "IP_ADDRESS";
    public final static int IN_COOKIE = 2;
    public final static String IN_COOKIE_STR = "IN_COOKIE";
    public final static int OUT_COOKIE = 3;
    public final static String OUT_COOKIE_STR = "OUT_COOKIE";
    public final static int GET_REQUEST = 4;
    public final static String GET_REQUEST_STR = "URL_REQUEST";
    public final static int BROWSER = 5;
    public final static String BROWSER_STR = "USER_AGENT";
    public final static int HTML_ERROR_CODE = 6;
    public final static String HTML_ERROR_CODE_STR = "HTML_ERROR_CODE";
    public final static int HIT_DATE_TIME = 8;
    public final static String HIT_DATE_TIME_STR = "HIT_DATE_TIME";
    public final static int BYTES_SENT = 14;
    public final static String BYTES_SENT_STR = "BYTES_SENT";
    public final static int SERVE_TIME = 15;
    public final static String SERVE_TIME_STR = "SERVE_TIME";
    public final static int CANONICAL_PORT = 16;
    public final static String CANONICAL_PORT_STR = "CANONICAL_PORT";
    public final static String REQUEST_PROTOCOL_STR = "REQUEST_PROTOCOL";
    public final static int REQUEST_PROTOCOL = 18;
    public final static String REFERRER_URL_STR = "REFERRER_URL";
    public final static int REFERRER_URL = 17;
    public final static int OTHER = 7;
    public final static String SERVER_NAME_STR = "SERVER_NAME";
    public final static int SERVER_NAME = 20;
    public final static String REMOTE_USER_STR = "REMOTE_USER";
    public final static int REMOTE_USER = 21;
    public final static String REQUEST_METHOD_STR = "REQUEST_METHOD";
    public final static int REQUEST_METHOD = 22;
    public final static String QUERY_STRING_STR = "QUERY_STRING";
    public final static int QUERY_STRING = 23;
    public final static String REQUEST_STRING_STR = "REQUEST_STRING";
    public final static int REQUEST_STRING = 24;
    public final static String CUSTOM_FIELD_1_STR = "CUSTOMFIELD1";
    public final static int CUSTOM_FIELD_1 = 25;
    public final static String CUSTOM_FIELD_2_STR = "CUSTOMFIELD2";
    public final static int CUSTOM_FIELD_2 = 26;
    public final static String CUSTOM_FIELD_3_STR = "CUSTOMFIELD3";
    public final static int CUSTOM_FIELD_3 = 27;
    public final static String OTHER_STR = "";
    private final static String[] OBJECT_TYPES = { null, IP_ADDRESS_STR, IN_COOKIE_STR, OUT_COOKIE_STR,
            GET_REQUEST_STR, BROWSER_STR, HTML_ERROR_CODE_STR, OTHER_STR, HIT_DATE_TIME_STR, null, null, null, null,
            null, BYTES_SENT_STR, SERVE_TIME_STR, CANONICAL_PORT_STR, REFERRER_URL_STR, REQUEST_PROTOCOL_STR, null,
            SERVER_NAME_STR, REMOTE_USER_STR, REQUEST_METHOD_STR, QUERY_STRING_STR, REQUEST_STRING_STR,
            CUSTOM_FIELD_1_STR, CUSTOM_FIELD_2_STR, CUSTOM_FIELD_3_STR };

    public final static int resolveObjectNameToID(String psObjectName) {
        if (psObjectName == null) {
            return -1;
        }

        for (int i = 0; i < OBJECT_TYPES.length; i++) {
            if ((OBJECT_TYPES[i] != null) && psObjectName.equalsIgnoreCase(OBJECT_TYPES[i])) {
                return i;
            }
        }

        return -1;
    }

    public final static String resolveObjectIDToName(int psObjectType) {
        if ((psObjectType >= 0) && (psObjectType < OBJECT_TYPES.length)) {
            return OBJECT_TYPES[psObjectType];
        }

        return null;
    }

    private final static Object[][] CONSTANTS = { { "ASC", "1" }, { "DESC", "0" } };

    public final static int resolveValueFromConstant(String psConstantName, int piDefault) {
        if (psConstantName == null) {
            return piDefault;
        }

        for (int i = 0; i < CONSTANTS.length; i++) {
            Object[] o = CONSTANTS[i];

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

    public final static int APACHE = 1;
    public static String APACHE_STR = "APACHE";
    public static String IPLANET_STR = "IPLANET";
    public static String IIS_STR = "IIS";
    public static String NETSCAPE_STR = "NETSCAPE";
    public final static int NETSCAPE = 2;
    public final static int MAX_WEBSERVERS = 3;

    public final static int resolveWebServerNameToID(String psWebServerName) {
        if (psWebServerName == null) {
            return -1;
        }
        else if (psWebServerName.equalsIgnoreCase(APACHE_STR) || psWebServerName.equalsIgnoreCase(IIS_STR)) {
            return APACHE;
        }
        else if (psWebServerName.equalsIgnoreCase(IPLANET_STR) || psWebServerName.equalsIgnoreCase(NETSCAPE_STR)) {
            return NETSCAPE;
        }
        else {
            return APACHE;
        }
    }

    /** database field lengths * */
    public final static int MAX_BROWSER_LENGTH = 255;
    public final static int MAX_REQUEST_LENGTH = 2000;
    public final static int MAX_REFERRER_LENGTH = 1000;
    public final static int MAX_MAIN_SESSION_IDENTIFIER_LENGTH = 100;
    public final static int MAX_FIRST_CLICK_SESSION_IDENTIFIER_LENGTH = 100;
    public final static int MAX_START_PERSISTANT_IDENTIFIER_LENGTH = 100;
    public final static int MAX_PERSISTANT_IDENTIFIER_LENGTH = 100;
    public final static int MAX_KEEP_VARIABLE_LENGTH = 255;

    /** string constants * */
    public final static String INVALID_MAIN_SESSION_IDENTIFIER_STRING = "-invalid-";

    /** other default database values * */
    public final static int MAX_STATEMENTS_PER_CONNECTION = 400;
    public static int MAX_ERROR_MESSAGE_LENGTH = 800;
    public static String VARIABLE_PARAMETER_START = "!@#";
    public static String VARIABLE_PARAMETER_END = "#@!";
    public static final String[] PARAMETER_JOB_EXECUTION_ID = { "DM_LOAD_ID", "JOB_EXEC_ID" };
    public static final String[] PARAMETER_LOAD_ID = { "LOAD_ID" };
    public static final String[] PARAMETER_DATE = { "DATE", "TIMESTAMP", "TIME" };
    public static final String[] PARAMETER_DATE_FORMAT = { "dd-MM-yyyy", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss" };
    public static final int JOB_PERSISTENCE = 0;
    public static final int LOAD_PERSISTENCE = 1;
    public static final int STATIC_PERSISTENCE = 2;
    private static String DEFAULTCACHESIZE = "64kb";
    private static String LOOKUPCLASS = null;
    public static String PARAMETER_LIST = "PARAMETER_LIST";
    public static String PARAMETER = "PARAMETER";
    public static String BAD_RECORD_PATH = "log";
    public static String CACHE_PATH = ".";
    private static String VERSION;
    private static double CACHEMEMRATIO = 0.5;

    private static Document mSystemXML = _getSystemXML();

    /**
     * EngineConstants constructor comment.
     */
    public EngineConstants() {
        super();
    }

    private static Element globals = null;

    private static synchronized Document _getSystemXML() {

        Document doc = null;

        try {
            // get system xml
            doc = XMLHelper.readXMLFromFile(Metadata.getKETLPath() + File.separator + Metadata.SYSTEM_FILE);
            // get all plugins

            globals = (Element) XMLHelper.findElementByName(doc, "GLOBAL", null, null);
            if (globals != null) {
                Element e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "PARAMETERSTART");
                if (e != null) {
                    EngineConstants.VARIABLE_PARAMETER_START = XMLHelper.getTextContent(e);
                }

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "VERSION");
                if (e != null) {
                    EngineConstants.VERSION = XMLHelper.getTextContent(e);
                    // Respectfully do not modify the following line of code without prior written permission from
                    // Kinetic Networks Inc.
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "KETL Version "
                            + VERSION + ", ©" + Calendar.getInstance().get(Calendar.YEAR) + " Kinetic Networks Inc.");
                    // End of section
                }

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "CACHEMEMRATIO");
                if (e != null) {
                    EngineConstants.CACHEMEMRATIO = Double.parseDouble(XMLHelper.getTextContent(e));
                }

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "INMEMORYCACHESIZE");
                if (e != null) {
                    String tmp = XMLHelper.getTextContent(e);

                    if (tmp == null)
                        tmp = "64k";

                    try {
                        NumberFormatter.convertToBytes(tmp);
                    } catch (Exception e2) {
                        System.err.println("Default in memory cache size invalid: " + tmp + ", defaulting to 64k");
                        tmp = "64k";
                    }
                    EngineConstants.DEFAULTCACHESIZE = tmp;
                }
                else
                    EngineConstants.DEFAULTCACHESIZE = "64k";

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "LOOKUPCLASS");

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
                    for (int i = 0; i < lookupsOptions.length; i++) {
                        try {
                            Class.forName(lookupsOptions[i][0]);
                            Class.forName(lookupsOptions[i][1]);

                            EngineConstants.LOOKUPCLASS = lookupsOptions[i][0];

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

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "PARAMETEREND");
                if (e != null) {
                    XMLHelper.getTextContent(e);
                    EngineConstants.VARIABLE_PARAMETER_END = XMLHelper.getTextContent(e);
                }

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "MAXERRORMESSAGELENGTH");
                if (e != null) {
                    try {
                        EngineConstants.MAX_ERROR_MESSAGE_LENGTH = Integer.parseInt(XMLHelper.getTextContent(e));
                    } catch (Exception e1) {

                    }
                }

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "BADRECORDPATH");
                if (e != null) {
                    try {
                        EngineConstants.BAD_RECORD_PATH = XMLHelper.getTextContent(e);

                        File f = new File(BAD_RECORD_PATH);
                        if (f.exists() == false) {
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                                    "Creating bad record directory " + f.getAbsolutePath());
                            f.mkdir();
                        }
                        else if (f.exists() && f.isDirectory() == false) {
                            System.err
                                    .println("Cannot initialize as bad record directory, as it is currently a file and not a directory: "
                                            + f.getAbsolutePath());
                            System.err.println("Please move this file or rename it: " + f.getAbsolutePath());
                        }
                    } catch (Exception e1) {

                    }
                }

                e = (Element) XMLHelper.getElementByName(globals, "OPTION", "NAME", "CACHEPATH");
                if (e != null) {
                    try {
                        EngineConstants.CACHE_PATH = XMLHelper.getTextContent(e);

                        File f = new File(CACHE_PATH);
                        if (f.exists() == false) {
                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                                    "Creating cache directory " + f.getAbsolutePath());
                            f.mkdir();
                        }
                        else if (f.exists() && f.isDirectory() == false) {
                            System.err
                                    .println("Cannot initialize cache record directory, as it is currently a file and not a directory: "
                                            + f.getAbsolutePath());
                            System.err.println("Please move this file or rename it: " + f.getAbsolutePath());
                        }
                    } catch (Exception e1) {

                    }
                }
            }

            File dir = new File(Metadata.getKETLPath() + File.separator + "xml" + File.separator + "plugins");

            if (dir.isDirectory()) {
                String[] children = dir.list();
                if (children == null) {
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE, "0 plugins found");
                }
                else {
                    for (int i = 0; i < children.length; i++) {
                        // Get filename of file or directory
                        if (children[i].endsWith(".xml") && new File(dir, children[i]).isFile()) {
                            try {
                                Document pluginDoc = XMLHelper.readXMLFromFile(dir.getAbsolutePath() + File.separator
                                        + children[i]);

                                Node[] node = XMLHelper.findElementsByName(pluginDoc, "STEP", null, null);
                                for (int p = 0; p < node.length; p++) {
                                    String pluginName = XMLHelper.getAttributeAsString(node[p].getAttributes(),
                                            "CLASS", null);
                                    if (pluginName == null) {
                                        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                                                "Plugin in file " + children[i] + " does not have a name.");
                                    }
                                    else {
                                        try {                                            
                                            Class.forName(pluginName);
                                            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                                                    "Plugin " + pluginName + " enabled.");
                                            doc.getFirstChild().appendChild(doc.importNode(node[p], true));
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

    public static synchronized Document getSystemXML() {
        if (mSystemXML == null) {
            mSystemXML = _getSystemXML();
        }

        return mSystemXML;
    }

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

    public static String replaceParameter(String strAction, String pParameterToLookFor, String[] pNewValueFormat,
            String[] pNewValue, String defaultValue) {
        int loadIDPos = 0;

        if ((pNewValueFormat == null) || (pNewValueFormat.length == 0)) {
            return replaceParameter(strAction, pParameterToLookFor, defaultValue);
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
     * @description Return list of parameters in text
     * @param strText
     * @return list of parameters
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

    public static String getVersion() {
        if (EngineConstants.VERSION == null)
            return "N/A";

        return EngineConstants.VERSION;
    }

    public static double getCacheMemoryRatio() {
        return CACHEMEMRATIO;
    }

    public static String getDefaultLookupClass() {
        return LOOKUPCLASS;
    }

    public static String getDefaultCacheSize() {
        return DEFAULTCACHESIZE;
    }
}
