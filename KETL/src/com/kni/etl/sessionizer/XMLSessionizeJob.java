/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.ETLJob;
import com.kni.etl.ETLJobStatus;
import com.kni.etl.EngineConstants;
import com.kni.etl.KETLJobStatus;
import com.kni.etl.SourceFieldDefinition;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl_v1.ParallelInlineSortFileReader;
import com.kni.etl.stringtools.StringMatcher;
import com.kni.etl.util.XMLHelper;

public class XMLSessionizeJob extends ETLJob  {

    public static final int FILE = 1;
    public static final int FILES = 1;
    public static final int HIT_CONNECTION = 1;
    public static final String PARALLISM = "PARALLISM";
    public static final String QA_AMOUNT_CLASSNAME = "com.kni.etl.ketl.qa.QAFileAmount";
    public static final String QA_ITEM_CHECK_CLASSNAME = "com.kni.etl.ketl.qa.QAItem";
    public static final String QA_RECOR_CHECK_CLASSNAME = "com.kni.etl.ketl.qa.QARecord";
    public static final String QA_SIZE_CLASSNAME = "com.kni.etl.ketl.qa.QAFileSize";
    public static final String QA_STRUCTURE_CLASSNAME = "com.kni.etl.ketl.qa.QAFileStructure";
    public static final Integer READER = new Integer(0);
    public static final String RESTART = "RESTART";
    public static final String SEARCHPATH = "SEARCHPATH";
    public static final int SESSION_CONNECTION = 0;
    public static final String SMP = "SMP";
    public static final String STORE_OPEN_SESSIONS = "STOREOPENSESSIONS";
    public static final int STREAM_PARSER = 0;
    public static final Integer WRITER = new Integer(1);

    static int storeParameterSet(ArrayList aParametersAndValues, ArrayList aParameters) {
        String[][] parametersToStore = new String[aParametersAndValues.size()][2];

        // parse values for any parameter substitution
        for (int i = 0; i < aParametersAndValues.size(); i++) {
            Object[] tmp = (Object[]) aParametersAndValues.get(i);

            for (int x = 0; x < aParametersAndValues.size(); x++) {
                // Copy element to prevent overwriting of variables in
                // duplciate lists.
                Object[] o = (Object[]) aParametersAndValues.get(x);

                Object[] oTmp = new Object[o.length];

                System.arraycopy(o, 0, oTmp, 0, o.length);

                o = oTmp;
                aParametersAndValues.set(x, o);

                if ((o[1] != null) && (tmp[1] != null)) {
                    o[1] = EngineConstants.replaceParameter((String) o[1], (String) tmp[0], (String) tmp[1]);
                }
            }
        }

        // parse values for any parameter substitution
        for (int i = 0; i < aParametersAndValues.size(); i++) {
            Object[] tmp = (Object[]) aParametersAndValues.get(i);

            if (tmp[0] != null) {
                parametersToStore[i][0] = new String((String) tmp[0]);
            }

            if (tmp[1] != null) {
                // if any variables not parsed then do not add list and return
                if (((String) tmp[1]).indexOf(EngineConstants.VARIABLE_PARAMETER_START) != -1) {
                    return 0;
                }

                parametersToStore[i][1] = new String((String) tmp[1]);
            }
        }

        aParameters.add(parametersToStore);

        return 1;
    }

    DocumentBuilderFactory dmfFactory;
    int iUpdateCount = -1;
    Object[][] maHitColumnMaps = null;
    ArrayList maHitParameters;
    ArrayList maLogFileParameters;
    ArrayList maMetadataParameters;
    Object[][] maSessionColumnMaps = null;
    ArrayList maSessionParameters;
    boolean mbHitCountRequired = false;
    boolean mbIgnoreLastRecord;
    boolean mbPagesOnly = false;
    int mCommitBatchSize = 1000;
    transient BlockingQueue mHitDataQueue = null;
    transient HashMap mHitQueueObjects = new HashMap();
    IDCounter mIDCounter;
    Node mnDestinations;
    Node mnIdentifiers;
    Node mnLogFiles;
    Node mNode = null;
    Node mnPageDefinitions;
    Node mnPageParameterSets;
    private PageParserPageDefinition[] mPageParserDefinition;
    private ArrayList mParallelChannelParser;
    int mPARALLISM = 1;
    boolean mRestartSessions = true;

    String[] msDBTags = { DBConnection.USER_ATTRIB, DBConnection.PASSWORD_ATTRIB, DBConnection.URL_ATTRIB, DBConnection.DRIVER_ATTRIB };

    transient BlockingQueue mSessionDataQueue = null;
    private SessionDefinition mSessionDefinition;
    transient HashMap mSessionQueueObjects = new HashMap();
    SourceFieldDefinition[] msfDefinition;
    String msHitSQLHint = null;
    String msHitTable = null;
    String msHitWriterClass = null;
    boolean mSkipInserts = false;
    int mSleepQueueSize = 10000;
    String[] msLogTags = { SEARCHPATH };
    boolean mSMP = false;
    String msSessionSQLHint = null;
    String msSessionTable = null;
    String msSessionWriterClass = null;
    boolean mStoreOpenSessionsAtEnd = true;
    int mWaitQueueSize = 100;

    /**
     * Insert the method's description here. Creation date: (5/4/2002 4:47:29 PM)
     * 
     * @param strSQL java.lang.String
     */
    public XMLSessionizeJob() throws Exception {
        super(new KETLJobStatus());
        dmfFactory = DocumentBuilderFactory.newInstance();
    }

    /**
     * @param pjsStatus
     */
    public XMLSessionizeJob(ETLJobStatus pjsStatus) throws Exception {
        this();
    }

    public synchronized void addHitDataQueueReader(Object pObject) {
        this.mHitQueueObjects.put(pObject, READER);
    }

    public synchronized void addSessionDataQueueReader(Object pObject) {
        this.mSessionQueueObjects.put(pObject, READER);
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 2:28:24 PM)
     */
    public void cleanup() {
        // If we still have a ResultSet open, we should close it...
    }

    public int closeOutSessionModeType() {
        if (restartSessions()) {
            if (this.mStoreOpenSessionsAtEnd) {
                return AnalyzeSessionQueuedThread.RESTART_SESSIONS_AND_STORE;
            }

            return AnalyzeSessionQueuedThread.RESTART_SESSIONS_NO_STORE;
        }

        if (this.mStoreOpenSessionsAtEnd) {
            return AnalyzeSessionQueuedThread.DISABLE_RESTART_AND_STORE;
        }

        return AnalyzeSessionQueuedThread.DISABLE_RESTART_NO_STORE;
    }

    private void createDestinationDefinition() throws Exception {
        NodeList nl = mnDestinations.getChildNodes();
        Node n = null;

        // See if we can match the name of the datasource...
        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo("SESSION") == 0) {
                n = nl.item(i);

                msSessionTable = XMLHelper.getAttributeAsString(n.getAttributes(), "TABLENAME", null);
                msSessionSQLHint = XMLHelper.getAttributeAsString(n.getAttributes(), "HINT", "");

                msSessionWriterClass = XMLHelper.getAttributeAsString(n.getAttributes(), "CLASS",
                        "com.kni.etl.sessionizer.SessionDatabaseWriter");

                String strParamListName = XMLHelper.getAttributeAsString(n.getAttributes(), "PARAMETER_LIST", null);

                if (strParamListName != null) {
                    recurseParameterList(this.mNode, strParamListName, new ArrayList(), new ArrayList(), this.msDBTags,
                            this.maSessionParameters);

                    msSessionTable = replaceParameter(msSessionTable, SESSION_CONNECTION);
                }

                maSessionColumnMaps = getDestinatinColumnMappings(n, "SESSION_IN");

                // calculate if hit count required. if not then we can run a little faster
                if (maSessionColumnMaps != null) {
                    for (int x = 0; x < maSessionColumnMaps.length; x++) {
                        String colName = (String) maSessionColumnMaps[x][1];

                        if ((colName != null) && colName.equalsIgnoreCase(SessionizationWriterRoot.getHitColumnName())) {
                            this.mbHitCountRequired = true;
                        }
                    }
                }
            }
            else if (nl.item(i).getNodeName().compareTo("HIT") == 0) {
                n = nl.item(i);

                msHitTable = XMLHelper.getAttributeAsString(n.getAttributes(), "TABLENAME", null);
                msHitSQLHint = XMLHelper.getAttributeAsString(n.getAttributes(), "HINT", "");

                msHitWriterClass = XMLHelper.getAttributeAsString(n.getAttributes(), "CLASS",
                        "com.kni.etl.sessionizer.HitDatabaseWriter");

                String strParamListName = XMLHelper.getAttributeAsString(n.getAttributes(), "PARAMETER_LIST", null);

                if (strParamListName != null) {
                    recurseParameterList(this.mNode, strParamListName, new ArrayList(), new ArrayList(), this.msDBTags,
                            this.maHitParameters);

                    msHitTable = replaceParameter(msHitTable, HIT_CONNECTION);
                }

                // should pages be parsed only.
                mbPagesOnly = XMLHelper.getAttributeAsBoolean(n.getAttributes(), "PAGESONLY", false);

                maHitColumnMaps = getDestinatinColumnMappings(n, "HIT_IN");
            }
        }

        return;
    }

    public ParallelInlineSortFileReader createFileChannelParsers() throws Exception {
        NodeList nl;

        nl = this.mnLogFiles.getChildNodes();

        for (int iLogFile = 0; iLogFile < nl.getLength(); iLogFile++) {
            if (nl.item(iLogFile).getNodeName().compareTo("LOG_FILE") == 0) {
                Node nLogFile = nl.item(iLogFile);

                // TODO: Add catch for wrong parameter list name
                String strParamList = XMLHelper.getAttributeAsString(nLogFile.getAttributes(), "PARAMETER_LIST", null);
                recurseParameterList(this.mNode, strParamList, new ArrayList(), new ArrayList(), this.msLogTags,
                        maLogFileParameters);

                ArrayList fileList = new ArrayList();

                for (int x = 0; x < maLogFileParameters.size(); x++) {
                    String searchPath = getParameterValue(maLogFileParameters, x, "SEARCHPATH");

                    if (searchPath != null) {
                        fileList = getWebLogFilenames(searchPath, fileList);
                    }
                }

                ParallelInlineSortFileReader parser = new ParallelInlineSortFileReader(nLogFile, nLogFile
                        .getChildNodes());
                parser.addFiles(fileList);
                return parser;
            }
        }

        return null;
    }

    private PageParserPageParameter[] createPageParameters(String parameterSetName) {
        ArrayList apDefs = new ArrayList();

        if (parameterSetName == null) {
            return null;
        }

        Node node = XMLHelper.getElementByName(mnPageParameterSets, "PAGE_PARAMETER_SET", "NAME", parameterSetName);

        if (node == null) {
            return null;
        }

        NodeList nl = node.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo("PAGE_PARAMETER") == 0) {
                Node n = nl.item(i);
                NamedNodeMap nmAttrs = n.getAttributes();
                PageParserPageParameter pageParameter = new PageParserPageParameter();

                pageParameter.setParameterName(XMLHelper.getAttributeAsString(nmAttrs, "NAME", null));
                pageParameter.setParameterRequired(XMLHelper.getAttributeAsBoolean(nmAttrs, "REQUIRED", false));
                pageParameter.setParameterValue(XMLHelper.getAttributeAsString(nmAttrs, "VALUE", null));
                pageParameter.setRemoveParameterValue(XMLHelper.getAttributeAsBoolean(nmAttrs, "REMOVE_VALUE", false));
                pageParameter.setRemoveParameter(XMLHelper.getAttributeAsBoolean(nmAttrs, "REMOVE_PARAMETER", false));
                pageParameter.setValueSeperator(XMLHelper.getAttributeAsString(nmAttrs, "SEPERATOR", null));

                apDefs.add(pageParameter);
            }
        }

        if (apDefs.size() == 0) {
            return null;
        }

        Object[] res = apDefs.toArray();
        PageParserPageParameter[] pageParameters = new PageParserPageParameter[res.length];

        System.arraycopy(res, 0, pageParameters, 0, res.length);

        return pageParameters;
    }

    private PageParserPageDefinition[] createPageParserParameters() {
        ArrayList apDefs = new ArrayList();

        NodeList nl;
        nl = this.mnPageDefinitions.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo("PAGE_DEFINITION") == 0) {
                Node n = nl.item(i);
                PageParserPageDefinition pDef = new PageParserPageDefinition();

                pDef.setWeight(XMLHelper.getAttributeAsInt(n.getAttributes(), "PRIORITY", 0));
                pDef.setProtocol(XMLHelper.getAttributeAsString(n.getAttributes(), "PROTOCOL", "*"));
                pDef.setHostName(XMLHelper.getAttributeAsString(n.getAttributes(), "HOSTNAME", "*"));
                pDef.setDirectory(XMLHelper.getAttributeAsString(n.getAttributes(), "DIRECTORY", null));
                pDef.setMethod(XMLHelper.getAttributeAsString(n.getAttributes(), "METHOD", "*"));
                pDef.setTemplate(XMLHelper.getAttributeAsString(n.getAttributes(), "TEMPLATE", null));
                pDef.setValidStatus(XMLHelper.getAttributeAsString(n.getAttributes(), "STATUSCODES", null));
                pDef.setValidPage(XMLHelper.getAttributeAsBoolean(n.getAttributes(), "VALID", true));
                pDef.setID(XMLHelper.getAttributeAsInt(n.getAttributes(), "ID", 0));

                String parameterSetName = XMLHelper.getAttributeAsString(n.getAttributes(), "PAGE_PARAMETER_SET", null);

                pDef.setValidPageParameters(createPageParameters(parameterSetName));

                apDefs.add(pDef);
            }
        }

        if (apDefs.size() == 0) {
            return null;
        }

        Object[] res = apDefs.toArray();
        PageParserPageDefinition[] pageDefinitions = new PageParserPageDefinition[res.length];

        System.arraycopy(res, 0, pageDefinitions, 0, res.length);

        return pageDefinitions;
    }

    private SessionDefinition createSessionDefinition() {
        // set field definition
        SessionDefinition srcSessionDefinition = new SessionDefinition();

        // set peak sessions an hour
        srcSessionDefinition.PeakSessionsAnHour = XMLHelper.getAttributeAsInt(this.mNode.getAttributes(),
                "PEAKSESSIONSANHOUR", 1800);

        // get main webserver type
        srcSessionDefinition.WebServerType = EngineConstants.resolveWebServerNameToID(XMLHelper.getAttributeAsString(
                this.mNode.getAttributes(), "WEBSERVERTYPE", null));

        // set default timeouts
        srcSessionDefinition.TimeOut = XMLHelper.getAttributeAsInt(this.mNode.getAttributes(), "TIMEOUT", 1800);

        mSMP = XMLHelper.getAttributeAsBoolean(this.mNode.getAttributes(), "SMP", false);
        mPARALLISM = XMLHelper.getAttributeAsInt(this.mNode.getAttributes(), "PARALLISM", 1);

        mSleepQueueSize = XMLHelper.getAttributeAsInt(this.mNode.getAttributes(), "SLEEPQUEUESIZE", 1000);
        mWaitQueueSize = XMLHelper.getAttributeAsInt(this.mNode.getAttributes(), "WAITQUEUESIZE", 100);

        mSkipInserts = XMLHelper.getAttributeAsBoolean(this.mNode.getAttributes(), "SKIPINSERTS", false);

        NodeList nl = this.mnIdentifiers.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo("IDENTIFIER_SET") == 0) {
                Node n = nl.item(i);
                NodeList nlIdentifiers = n.getChildNodes();

                String type = XMLHelper.getAttributeAsString(n.getAttributes(), "TYPE", null);
                boolean enableFallBack = XMLHelper.getAttributeAsBoolean(n.getAttributes(), "ENABLEFALLBACK", false);

                boolean expireWhenBetterMatch = XMLHelper.getAttributeAsBoolean(n.getAttributes(),
                        "EXPIREWHENBETTERMATCH", false);

                int priority = XMLHelper.getAttributeAsInt(n.getAttributes(), "PRIORITY", 0);
                int timeOut = XMLHelper.getAttributeAsInt(n.getAttributes(), "TIMEOUT", srcSessionDefinition.TimeOut);

                int DestinationObjectType = -1;

                if (type.equalsIgnoreCase("MAIN")) {
                    if ((srcSessionDefinition.MainIdentifierTimeOut == 0)) {
                        srcSessionDefinition.MainIdentifierTimeOut = timeOut;
                    }

                    srcSessionDefinition.MainIdentifierFallbackEnabled = enableFallBack;
                    srcSessionDefinition.MainIdentifierExpireWhenBetterMatch = expireWhenBetterMatch;
                    DestinationObjectType = EngineConstants.SESSION_MAIN_SESSION_IDENTIFIER;
                }
                else if (type.equalsIgnoreCase("IP_BROWSER")) {
                    if (srcSessionDefinition.IPBrowserTimeOut == 0) {
                        srcSessionDefinition.IPBrowserTimeOut = timeOut;
                    }

                    srcSessionDefinition.IPBrowserFallbackEnabled = enableFallBack;
                    srcSessionDefinition.IPBrowserExpireWhenBetterMatch = expireWhenBetterMatch;
                    DestinationObjectType = -1;
                }
                else if (type.equalsIgnoreCase("PERSISTANT")) {
                    if (srcSessionDefinition.PersistantIdentifierTimeOut == 0) {
                        srcSessionDefinition.PersistantIdentifierTimeOut = timeOut;
                    }

                    srcSessionDefinition.PersistantIdentifierFallbackEnabled = enableFallBack;
                    srcSessionDefinition.PersistantIdentifierExpireWhenBetterMatch = expireWhenBetterMatch;

                    DestinationObjectType = EngineConstants.SESSION_PERSISTANT_IDENTIFIER;
                }
                else if (type.equalsIgnoreCase("FIRST_CLICK")) {
                    if (srcSessionDefinition.FirstClickIdentifierTimeOut == 0) {
                        srcSessionDefinition.FirstClickIdentifierTimeOut = timeOut;
                    }

                    srcSessionDefinition.FirstClickIdentifierExpireWhenBetterMatch = expireWhenBetterMatch;
                    srcSessionDefinition.FirstClickIdentifierFallbackEnabled = enableFallBack;
                    DestinationObjectType = EngineConstants.SESSION_FIRST_CLICK_IDENTIFIER;
                }
                else if (type.equalsIgnoreCase("KEEP_VARIABLE")) {
                    DestinationObjectType = EngineConstants.KEEP_COOKIE_VARIABLE;
                }

                for (int idx = 0; idx < nlIdentifiers.getLength(); idx++) {
                    if (nlIdentifiers.item(idx).getNodeName().compareTo("IDENTIFIER") == 0) {
                        SessionIdentifier sId = new SessionIdentifier();
                        Node nlIdentifier = nlIdentifiers.item(idx);
                        String objectType = XMLHelper.getAttributeAsString(nlIdentifier.getAttributes(), "OBJECTTYPE",
                                null);
                        sId.ObjectType = EngineConstants.resolveObjectNameToID(objectType);

                        if ((DestinationObjectType == -1) && (sId.ObjectType == EngineConstants.IP_ADDRESS)) {
                            sId.DestinationObjectType = EngineConstants.SESSION_IP_ADDRESS;
                        }
                        else if ((DestinationObjectType == -1) && (sId.ObjectType == EngineConstants.BROWSER)) {
                            sId.DestinationObjectType = EngineConstants.SESSION_BROWSER;
                        }
                        else {
                            sId.DestinationObjectType = DestinationObjectType;
                        }

                        sId.setVariableName(XMLHelper.getAttributeAsString(nlIdentifier.getAttributes(),
                                "VARIABLENAME", null), XMLHelper.getAttributeAsBoolean(nlIdentifier.getAttributes(),
                                "CASESENSITIVE", false));

                        sId.Weight = priority;

                        // default to String
                        sId.DataType = com.kni.etl.ketl_v1.BaseDataItem.STRING;

                        srcSessionDefinition.addSessionIdentifier(sId);
                    }
                }
            }
        }

        return srcSessionDefinition;
    }

    /**
     * Insert the method's description here. Creation date: (5/9/2002 12:06:44 PM)
     */
    protected void finalize() throws Throwable {
        cleanup();

        // It's good practice to call the superclass's finalize() method,
        // even if you know there is not one currently defined...
        super.finalize();
    }

    /**
     * @return
     */
    public int getBatchCommitSize() {
        return mCommitBatchSize;
    }

    public String getClassForStep(String pCurrentStep) {
        if (pCurrentStep.equals("ReadFiles")) {
            return "ParallelChannelFileReaderThread";
        }
        else if (pCurrentStep.equals("AnalyzeHit")) {
            return "AnalyzeSessionThread";
        }
        else if (pCurrentStep.equals("WriteToDestination")) {
            return "DatabaseWriter";
        }

        return null;
    }

    public java.lang.String getDatabaseDriverClass(int pType) {
        return getParameterValueForType(pType, DBConnection.DRIVER_ATTRIB);
    }

    public java.lang.String getDatabasePassword(int pType) {
        return getParameterValueForType(pType, DBConnection.PASSWORD_ATTRIB);
    }

    public java.lang.String getDatabaseURL(int pType) {
        return getParameterValueForType(pType, DBConnection.URL_ATTRIB);
    }

    public java.lang.String getDatabaseUser(int pType) {
        return getParameterValueForType(pType, DBConnection.USER_ATTRIB);
    }

    Object[][] getDestinatinColumnMappings(Node n, String TagName) {
        NodeList nlOut = n.getChildNodes();

        ArrayList columnMappings = new ArrayList();

        for (int idx = 0; idx < nlOut.getLength(); idx++) {
            // See if we can match the name of the datasource...
            if (nlOut.item(idx).getNodeName().compareTo(TagName) == 0) {
                Node ns = nlOut.item(idx);
                Object[] o = new Object[2];

                o[0] = XMLHelper.getAttributeAsString(ns.getAttributes(), "NAME", null);
                o[1] = XMLHelper.getAttributeAsString(ns.getAttributes(), "SOURCE", null);

                columnMappings.add(o);
            }
        }

        // give file format configuration to parallel file reader.
        Object[] res = columnMappings.toArray();
        Object[][] columnMaps = new Object[res.length][];

        System.arraycopy(res, 0, columnMaps, 0, res.length);

        return columnMaps;
    }

    /**
     * @return
     */
    public Object[][] getHitColumnMaps() {
        return maHitColumnMaps;
    }

    /**
     * @return
     */
    public String getHitSQLHint() {
        return msHitSQLHint;
    }

    /**
     * @return
     */
    public String getHitTable() {
        return msHitTable;
    }

    /**
     * @return Returns the msHitWriterClass.
     */
    public String getHitWriterClass() {
        return msHitWriterClass;
    }

    public synchronized BlockingQueue getHitWriterDataQueue(Integer pType, Object pObject) {
        if (mHitDataQueue == null) {
            mHitDataQueue = new LinkedBlockingQueue(mWaitQueueSize);
        }

        this.mHitQueueObjects.put(pObject, pType);

        return mHitDataQueue;
    }

    public synchronized IDCounter getIDCounter() throws Exception {
        if ((this.mIDCounter == null) && (ResourcePool.getMetadata() == null)) {
            mIDCounter = new IDCounter();
        }
        else if (this.mIDCounter == null) {
            mIDCounter = new IDCounter("TEMP_SESSION_ID", 10000);
        }

        return this.mIDCounter;
    }

    Node getJobChildNodes() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getNextStep(String pCurrentStep) {
        if (pCurrentStep == null) {
            return "ReadFiles";
        }
        else if (pCurrentStep.equals("ReadFiles")) {
            return "AnalyzeHit";
        }
        else if (pCurrentStep.equals("AnalyzeHit")) {
            return "WriteToDestination";
        }
        else {
            return null;
        }
    }

    /**
     * @return
     */
    public PageParserPageDefinition[] getPageParserDefinition() {
        if (this.mPageParserDefinition == null)
            mPageParserDefinition = createPageParserParameters();

        return mPageParserDefinition;
    }

    /**
     * @return
     */
    public final int getParallism() {
        return mPARALLISM;
    }

    Object[] getParameterToFind(String pParameterName) {
        Object[] tmp = { pParameterName, null, null };

        return tmp;
    }

    String getParameterValue(ArrayList maParameters, int iParamList, String strParamName) {
        if ((maParameters != null) && (maParameters.isEmpty() == false)) {
            String[][] paramList = (String[][]) maParameters.get(iParamList);

            if (paramList == null) {
                return null;
            }

            for (int i = 0; i < paramList.length; i++) {
                String[] tmp = paramList[i];

                if ((tmp != null) && tmp[0].equalsIgnoreCase(strParamName)) {
                    return tmp[1];
                }
            }
        }

        return null;
    }

    public java.lang.String getParameterValueForType(int pType, String pParameter) {
        switch (pType) {
        case SESSION_CONNECTION:
            return this.getParameterValue(maSessionParameters, 0, pParameter);

        case HIT_CONNECTION:
            return this.getParameterValue(maHitParameters, 0, pParameter);

        default:
            return this.getParameterValue(maSessionParameters, 0, pParameter);
        }
    }

    /**
     * @return
     */
    public Object[][] getSessionColumnMaps() {
        return maSessionColumnMaps;
    }

    /**
     * @return
     */
    public SessionDefinition getSessionDefinition() {

        if (this.mSessionDefinition == null)
            mSessionDefinition = createSessionDefinition();

        return mSessionDefinition;
    }

    /**
     * @return
     */
    public String getSessionSQLHint() {
        return msSessionSQLHint;
    }

    /**
     * @return
     */
    public String getSessionTable() {
        return msSessionTable;
    }

    /**
     * @return Returns the msSessionWriterClass.
     */
    public String getSessionWriterClass() {
        return msSessionWriterClass;
    }

    public synchronized BlockingQueue getSessionWriterDataQueue(Integer pType, Object pObject) {
        if (mSessionDataQueue == null) {
            mSessionDataQueue = new LinkedBlockingQueue(mWaitQueueSize);
        }

        this.mSessionQueueObjects.put(pObject, pType);

        return mSessionDataQueue;
    }

    /**
     * @return
     */
    public final int getSleepQueueSize() {
        return mSleepQueueSize;
    }

    public SourceFieldDefinition[] getSourceFieldDefinition() {
        return msfDefinition;
    }

    /**
     * @return
     */
    public final int getWaitQueueSize() {
        return mWaitQueueSize;
    }

    public ArrayList getWebLogFilenames(String pSearchPath, ArrayList pDestinationList) {
        if (pSearchPath == null) {
            return (null);
        }

        int lastPos = pSearchPath.lastIndexOf("/");

        if (lastPos == -1) {
            lastPos = pSearchPath.lastIndexOf("\\");
        }

        if (lastPos > 0) {
            String dirStr = pSearchPath.substring(0, lastPos);
            String fileSearch = pSearchPath.substring(lastPos + 1);

            StringMatcher filePattern = null;

            if (fileSearch != null) {
                filePattern = new StringMatcher(fileSearch);
            }
            else {
                return null;
            }

            File dir = new File(dirStr);

            if (dir.exists() == false) {
                this.getStatus().setErrorMessage("Weblog search string doesn not exist" + dirStr);

                return null;
            }

            File[] list = dir.listFiles();

            for (int i = 0; i < list.length; i++) {
                if (list[i].isFile()) {
                    if (filePattern.match(list[i].getName())) {
                        if (pDestinationList == null) {
                            pDestinationList = new ArrayList();
                        }

                        pDestinationList.add(list[i].getPath());
                    }
                }
            }
        }

        return pDestinationList;
    }

    public String getWriterClassForChannel(int pChannel) {
        if (isSessionWriter(pChannel)) {
            return this.getSessionWriterClass();
        }

        return this.getHitWriterClass();
    }

    boolean hasCompleteParameterSet(ArrayList aParametersAndValues, String[] aRequiredTags) {
        if ((aRequiredTags == null) || (aRequiredTags.length == 0)) {
            ResourcePool.LogMessage(this, "Warning: No parameters defined for parameter set");

            return false;
        }

        for (int i = 0; i < aRequiredTags.length; i++) {
            boolean found = false;

            for (int x = 0; x < aParametersAndValues.size(); x++) {
                Object[] tmp = (Object[]) aParametersAndValues.get(x);

                if (((String) tmp[0]).equalsIgnoreCase(aRequiredTags[i])) {
                    found = true;
                }
            }

            if (found == false) {
                return false;
            }
        }

        return true;
    }

    public boolean isHitCountRequired() {
        return mbHitCountRequired;
    }

    public boolean isSessionWriter(int pChannel) {
        if (pChannel == 1) {
            return true;
        }

        return false;
    }

    /**
     * @return
     */
    public final boolean isSMP() {
        return mSMP;
    }

    public void loadDestinationSettings() throws Exception {
        createDestinationDefinition();
    }

    public synchronized boolean noMoreHitReaders() {
        return noMoreOfType(this.mHitQueueObjects, READER);
    }

    public synchronized boolean noMoreHitWriters() {
        return noMoreOfType(this.mHitQueueObjects, WRITER);
    }

    boolean noMoreOfType(HashMap pMap, Integer pType) {
        // scan hashmap
        for (Iterator i = pMap.keySet().iterator(); i.hasNext();) {
            Object o = i.next();

            if ((o != null) && (pMap.get(o) == pType)) {
                return false;
            }
        }

        return true;
    }

    public synchronized boolean noMoreSessionReaders() {
        return noMoreOfType(this.mSessionQueueObjects, READER);
    }

    public synchronized boolean noMoreSessionWriters() {
        return noMoreOfType(this.mSessionQueueObjects, WRITER);
    }

    public boolean pagesOnly() {
        return this.mbPagesOnly;
    }

    int recurseParameterList(Node xmlSourceNode, String strParameterListName, ArrayList aParameterValuesList,
            ArrayList aParentParameterLists, String[] aRequiredTags, ArrayList aParameters) {
        int res = 0;

        // Duplicate list and add current parameter list
        // this helps protect against loops
        ArrayList newArrayList = new ArrayList(aParentParameterLists);
        ArrayList newParameterValuesList = new ArrayList(aParameterValuesList);

        String[] parametersInList = XMLHelper.getDistinctParameterNames(xmlSourceNode, strParameterListName);

        if (parametersInList != null) {
            for (int i = 0; i < parametersInList.length; i++) {
                boolean doesntExist = true;

                for (int x = 0; x < newParameterValuesList.size(); x++) {
                    Object[] o = (Object[]) newParameterValuesList.get(x);

                    if ((o != null) && (o[0] != null) && (((String) o[0]).compareTo(parametersInList[i]) == 0)) {
                        doesntExist = false;
                    }
                }

                if (doesntExist) {
                    newParameterValuesList.add(getParameterToFind(parametersInList[i]));
                }
            }
        }

        for (int i = 0; i < newArrayList.size(); i++) {
            String tmp = (String) newArrayList.get(i);

            // check for loops, warn if exists
            if ((tmp != null) && (tmp.compareTo(strParameterListName) == 0)) {
                ResourcePool.LogMessage(this, "Warning: Loop exists in sub parameter list(" + strParameterListName
                        + ") pointing to itself at a lower level,"
                        + " no more sub parameter lists will be searched in this branch.");

                return 0;
            }
        }

        // add parameter list to list.
        newArrayList.add(strParameterListName);

        int values = 0;

        for (int i = 0; i < newParameterValuesList.size(); i++) {
            Object[] tmp = (Object[]) newParameterValuesList.get(i);

            if ((tmp != null) && (tmp[0] != null)) {
                tmp[1] = XMLHelper.getParameterValueAsString(xmlSourceNode, strParameterListName, (String) tmp[0],
                        (String) tmp[1]);

                if (tmp[1] != null) {
                    String[] extraParams = EngineConstants.getParametersFromText((String) tmp[1]);

                    if (extraParams != null) {
                        for (int p = 0; p < extraParams.length; p++) {
                            boolean doesntExist = true;

                            for (int x = 0; x < newParameterValuesList.size(); x++) {
                                Object[] o = (Object[]) newParameterValuesList.get(x);

                                if ((o != null) && (o[0] != null) && (((String) o[0]).compareTo(extraParams[p]) == 0)) {
                                    doesntExist = false;
                                }
                            }

                            if (doesntExist) {
                                newParameterValuesList.add(getParameterToFind(extraParams[p]));
                            }
                        }
                    }

                    values++;
                }

                tmp[2] = XMLHelper.getSubParameterListNames(xmlSourceNode, strParameterListName, (String) tmp[0]);
            }
        }

        if (hasCompleteParameterSet(newParameterValuesList, aRequiredTags)) {
            res = storeParameterSet(newParameterValuesList, aParameters) + res;
        }

        for (int i = 0; i < newParameterValuesList.size(); i++) {
            Object[] tmp = (Object[]) newParameterValuesList.get(i);

            if ((tmp != null) && (tmp[2] != null)) {
                String[] subLists = (String[]) tmp[2];

                for (int pos = 0; pos < subLists.length; pos++) {
                    res = res
                            + recurseParameterList(xmlSourceNode, subLists[pos], newParameterValuesList, newArrayList,
                                    aRequiredTags, aParameters);
                }
            }
        }

        return res;
    }

    public synchronized void releaseHitWriterDataQueue(Object pObject) {
        this.mHitQueueObjects.remove(pObject);
    }

    public synchronized void releaseReaderDataQueue(Object pObject) {
        this.mSessionQueueObjects.remove(pObject);
        this.mHitQueueObjects.remove(pObject);
    }

    public synchronized void releaseSessionWriterDataQueue(Object pObject) {
        this.mSessionQueueObjects.remove(pObject);
    }

    /**
     * @throws Exception
     */
    private String replaceParameter(String pString, int pType) throws Exception {
        String[] strParms = EngineConstants.getParametersFromText(pString);

        if (strParms != null) {
            for (int x = 0; x < strParms.length; x++) {
                String parmValue = this.getParameterValueForType(pType, strParms[x]);

                if (parmValue != null) {
                    pString = EngineConstants.replaceParameter(pString, strParms[x], parmValue);
                }
                else {
                    throw new Exception("Parameter " + strParms[x] + " can not be found in parameter list");
                }
            }
        }

        return pString;
    }

    public boolean restartSessions() {
        return this.mRestartSessions;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:34 PM)
     * 
     * @return java.lang.String
     */
    public final boolean runSMP() {
        return this.isSMP();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLJob#setAction(java.lang.Object)
     */
    public void setAction(Object oAction) throws Exception {
        super.setAction(oAction);

        DocumentBuilder builder = null;
        Document xmlDOM = null;
        NodeList nl;
        Node node;

        // Build a DOM out of the XML string...
        try {
            builder = dmfFactory.newDocumentBuilder();
            xmlDOM = builder.parse(new InputSource(new StringReader((String) this.getAction())));
        } catch (Exception e) {
            this.getStatus().setErrorCode(ETLJobStatus.PENDING_CLOSURE_FAILED); // BRIAN: NEED TO SET UP KETL JOB ERROR
            // CODES
            this.getStatus().setErrorMessage("Error reading job XML: " + e.getMessage());

            return;
        }

        nl = xmlDOM.getElementsByTagName("SESSIONIZER");

        if ((nl == null) || (nl.getLength() == 0)) {
            this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
            this.getStatus().setErrorMessage("Error reading job XML: no SESSIONIZER specified.");

            return;
        }

        if (nl.getLength() > 1) {
            this.getStatus().setErrorCode(2); // BRIAN: NEED TO SET UP KETL JOB ERROR CODES
            this.getStatus().setErrorMessage("Error reading job XML: more than 1 SESSIONIZER specified.");

            return;
        }

        // Create the step objects...
        for (int i = 0; i < nl.getLength(); i++) {
            node = nl.item(i);
            this.mNode = node;
        }

        nl = this.mNode.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            // See if we can match the name of the datasource...
            if (nl.item(i).getNodeName().compareTo("LOG_FILES") == 0) {
                mnLogFiles = nl.item(i);
            }
            else if (nl.item(i).getNodeName().compareTo("PAGE_DEFINITIONS") == 0) {
                mnPageDefinitions = nl.item(i);
            }
            else if (nl.item(i).getNodeName().compareTo("DESTINATION") == 0) {
                mnDestinations = nl.item(i);
            }
            else if (nl.item(i).getNodeName().compareTo("IDENTIFIERS") == 0) {
                mnIdentifiers = nl.item(i);
            }
            else if (nl.item(i).getNodeName().compareTo("PAGE_PARAMETER_SETS") == 0) {
                mnPageParameterSets = nl.item(i);
            }
        }

        maLogFileParameters = new ArrayList();
        maHitParameters = new ArrayList();
        maMetadataParameters = new ArrayList();
        maSessionParameters = new ArrayList();

        mCommitBatchSize = XMLHelper.getAttributeAsInt(this.mNode.getAttributes(), "BATCHCOMMITSIZE", 1000);

        mRestartSessions = XMLHelper.getAttributeAsBoolean(this.mNode.getAttributes(), RESTART, true);
        mStoreOpenSessionsAtEnd = XMLHelper
                .getAttributeAsBoolean(this.mNode.getAttributes(), STORE_OPEN_SESSIONS, true);

    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 6:13:26 PM)
     * 
     * @return int
     */
    public void setBatchCommitSize(String pParam) {
        setGlobalParameter("batch_commit_size", pParam);
    }

    protected Node setChildNodes(Node pParentNode) {
        // turn file into readable nodes
        DocumentBuilder builder = null;
        Document xmlConfig;
        Node e = null;

        try {
            DocumentBuilderFactory dmf = DocumentBuilderFactory.newInstance();
            builder = dmf.newDocumentBuilder();
            xmlConfig = builder.parse(new InputSource(new StringReader(this.getAction().toString())));
            e = pParentNode.getOwnerDocument().importNode(xmlConfig.getFirstChild(), true);
            pParentNode.appendChild(e);
        } catch (org.xml.sax.SAXException e2) {
            ResourcePool.LogMessage(Thread.currentThread(), "ERROR: parsing XML document, " + e2.toString());
            this.getStatus().setErrorCode(EngineConstants.INVALID_XML_EXIT_CODE);
        } catch (Exception e1) {
            ResourcePool.LogException(e1, this);
            this.getStatus().setErrorCode(EngineConstants.OTHER_ERROR_EXIT_CODE);
        }

        return e;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 5:56:56 PM)
     * 
     * @param newDriverClass java.lang.String
     */
    public void setDatabaseDriverClass(java.lang.String newDatabaseDriverClass) {
        setGlobalParameter("driver", newDatabaseDriverClass);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:58 PM)
     * 
     * @param newDatabasePassword java.lang.String
     */
    public void setDatabasePassword(java.lang.String newDatabasePassword) {
        setGlobalParameter("password", newDatabasePassword);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:00:10 PM)
     * 
     * @param newDatabaseURL java.lang.String
     */
    public void setDatabaseURL(java.lang.String newDatabaseURL) {
        setGlobalParameter("url", newDatabaseURL);
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:02:34 PM)
     * 
     * @param newDatabaseUser java.lang.String
     */
    public void setDatabaseUser(java.lang.String newDatabaseUser) {
        setGlobalParameter("user", newDatabaseUser);
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 6:09:22 PM)
     * 
     * @return java.lang.String
     */
    public void setFileDefinitionID(String pParam) {
        setGlobalParameter("file_definition_id", pParam);
    }

    public void setMetadataDriver(String newMD) {
        setGlobalParameter("md_driver", newMD);
    }

    public void setMetadataPassword(String newMD) {
        setGlobalParameter("md_pwd", newMD);
    }

    public void setMetadataURL(String newMD) {
        setGlobalParameter("md_url", newMD);
    }

    public void setMetadataUser(String newMD) {
        setGlobalParameter("md_user", newMD);
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 6:09:42 PM)
     * 
     * @return java.lang.String
     */
    public void setPageDefinitionID(String pParam) {
        setGlobalParameter("page_definition_id", pParam);
    }

    /**
     * @param i
     */
    public final void setParallism(int i) {
        mPARALLISM = i;
    }

    /**
     * Insert the method's description here. Creation date: (5/13/2002 6:08:59 PM)
     * 
     * @return java.lang.String
     */
    public void setSessionDefinitionID(String pParam) {
        setGlobalParameter("session_definition_id", pParam);
    }

    /**
     * @param i
     */
    public final void setSleepQueueSize(int i) {
        mSleepQueueSize = i;
    }

    /**
     * @param b
     */
    public final void setSMP(boolean b) {
        mSMP = b;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:53:09 PM)
     * 
     * @param newUpdateCount int
     */
    public void setUpdateCount(int newUpdateCount) {
        iUpdateCount = newUpdateCount;
    }

    /**
     * @param i
     */
    public final void setWaitQueueSize(int i) {
        mWaitQueueSize = i;
    }

    /**
     * Insert the method's description here. Creation date: (5/4/2002 6:00:10 PM)
     * 
     * @param newDatabaseURL java.lang.String
     */
    public void setWebLogSearchString(java.lang.String newParam) {
        setGlobalParameter("weblog_srch", newParam);
    }

    /**
     * @return
     */
    public boolean skipInserts() {
        return mSkipInserts;
    }




}
