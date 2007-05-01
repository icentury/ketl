/*
 * Created on Jul 13, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.splitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.smp.ETLSplit;
import com.kni.etl.ketl.smp.ETLSplitCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.sessionizer.AnalyzePageview;
import com.kni.etl.sessionizer.IDCounter;
import com.kni.etl.sessionizer.PageParserPageDefinition;
import com.kni.etl.sessionizer.PageParserPageParameter;
import com.kni.etl.sessionizer.Session;
import com.kni.etl.sessionizer.SessionDefinition;
import com.kni.etl.sessionizer.SessionIdentifier;
import com.kni.etl.sessionizer.AnalyzePageview.Holder;
import com.kni.etl.util.XMLHelper;
import com.kni.util.Arrays;

// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

public class Sessionizer extends ETLSplit {

    private static final String CASESENSITIVE = "CASESENSITIVE";
    private static final String VARIABLENAME = "VARIABLENAME";
    private static final String OBJECTTYPE = "OBJECTTYPE";
    private static final String IDENTIFIER = "IDENTIFIER";
    private static final String KEEP_VARIABLE = "KEEP_VARIABLE";
    private static final String FIRST_CLICK = "FIRST_CLICK";
    private static final String PERSISTANT = "PERSISTANT";
    private static final String IP_BROWSER = "IP_BROWSER";
    private static final String MAIN = "MAIN";
    private static final String EXPIREWHENBETTERMATCH = "EXPIREWHENBETTERMATCH";
    private static final String ENABLEFALLBACK = "ENABLEFALLBACK";
    private static final String TYPE = "TYPE";
    private static final String IDENTIFIER_SET = "IDENTIFIER_SET";
    private static final String ID = "ID";
    private static final String VALID = "VALID";
    private static final String TEMPLATE = "TEMPLATE";
    private static final String METHOD = "METHOD";
    private static final String DIRECTORY = "DIRECTORY";
    private static final String STATUSCODES = "STATUSCODES";
    private static final String HOSTNAME = "HOSTNAME";
    private static final String PROTOCOL = "PROTOCOL";
    private static final String PAGE_DEFINITION = "PAGE_DEFINITION";
    private static final String PRIORITY = "PRIORITY";
    private static final String REQUIRED = "REQUIRED";
    private static final String SEPERATOR = "SEPERATOR";
    private static final String REMOVE_PARAMETER = "REMOVE_PARAMETER";
    private static final String REMOVE_VALUE = "REMOVE_VALUE";
    private static final String VALUE = "VALUE";
    private static final String NAME = "NAME";
    private static final String PAGE_PARAMETER = "PAGE_PARAMETER";
    private static final String PAGE_PARAMETER_SET = "PAGE_PARAMETER_SET";
    private static final String PAGE_PARAMETER_SETS = "PAGE_PARAMETER_SETS";
    private static final String IDENTIFIERS = "IDENTIFIERS";
    private static final String PAGE_DEFINITIONS = "PAGE_DEFINITIONS";
    private static final String WEBSERVERTYPE = "WEBSERVERTYPE";
    private static final String STOREOPENSESSIONS = "STOREOPENSESSIONS";
    private static final String PAGESONLY = "PAGESONLY";
    private static final String RESTART = "RESTART";
    private static final String TIMEOUT = "TIMEOUT";
    private static final String PEAKSESSIONSANHOUR = "PEAKSESSIONSANHOUR";
    private int miPeakSessionsAnHour;
    private int miWebServerType;
    private int miTimeOut;
    private boolean mRestartSessions;
    private boolean mStoreOpenSessionsAtEnd;
    private Node mnPageDefinitions;
    private Node mnDestinations;
    private Node mnIdentifiers;
    private Node mnPageParameterSets;
    private boolean mbPagesOnly;

    public Sessionizer(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);

        if (res != 0)
            return res;

        this.miPeakSessionsAnHour = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(),
                Sessionizer.PEAKSESSIONSANHOUR, 1800);
        this.miWebServerType = EngineConstants.resolveWebServerNameToID(XMLHelper.getAttributeAsString(xmlConfig
                .getAttributes(), Sessionizer.WEBSERVERTYPE, null));
        this.miTimeOut = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), Sessionizer.TIMEOUT, 1800);
        this.mRestartSessions = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), Sessionizer.RESTART, false);
        this.mbPagesOnly = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), Sessionizer.PAGESONLY, false);

        this.mStoreOpenSessionsAtEnd = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(),
                Sessionizer.STOREOPENSESSIONS, true);

        // get sub details
        NodeList nl = xmlConfig.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo(Sessionizer.PAGE_DEFINITIONS) == 0) {
                this.mnPageDefinitions = nl.item(i);
            }
            else if (nl.item(i).getNodeName().compareTo(Sessionizer.IDENTIFIERS) == 0) {
                this.mnIdentifiers = nl.item(i);
            }
            else if (nl.item(i).getNodeName().compareTo(Sessionizer.PAGE_PARAMETER_SETS) == 0) {
                this.mnPageParameterSets = nl.item(i);
            }
        }

        // create metadata managed id counter
        IDCounter idCounter;
        try {
            idCounter = new IDCounter("TEMP_SESSION_ID", 10000);
        } catch (Exception e) {
            throw new KETLThreadException("Failed to get TEMP_SESSION_ID", e, this);
        }

        this.mCompleteSessionList = new ArrayList<Session>();
        // check for existance of restart file if reload sessions enabled
        if (this.mRestartSessions && ((this.mAnalyzePageview = this.restartJob()) != null)) {
            ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                    "Reloading non-final sessions from last run.");
        }
        else {
            this.mAnalyzePageview = null;
        }

        if (this.mAnalyzePageview == null) {
            this.mAnalyzePageview = new AnalyzePageview();
        }

        if (this.mRestartSessions)
            this.mAnalyzePageview
                    .setCloseOutMode(this.mStoreOpenSessionsAtEnd ? AnalyzePageview.RESTART_SESSIONS_AND_STORE
                            : AnalyzePageview.RESTART_SESSIONS_NO_STORE);
        else
            this.mAnalyzePageview
                    .setCloseOutMode(this.mStoreOpenSessionsAtEnd ? AnalyzePageview.DISABLE_RESTART_AND_STORE
                            : AnalyzePageview.DISABLE_RESTART_NO_STORE);

        this.mItemMap = new int[this.mInPorts.length];
        for (Object o : this.mInPorts) {
            SessionizerETLInPort port = (SessionizerETLInPort) o;
            this.mItemMap[port.getSourcePortIndex()] = port.miObjectType;
        }

        this.mAnalyzePageview.configure(idCounter, this.createSessionDefinition(), this.createPageParserParameters(),
                this.mbPagesOnly, this.mbHitsNeeded, this.mItemMap, this.mCompleteSessionList);

        return 0;
    }

    private PageParserPageParameter[] createPageParameters(String parameterSetName) {
        ArrayList apDefs = new ArrayList();

        if (parameterSetName == null) {
            return null;
        }

        Node node = XMLHelper.getElementByName(this.mnPageParameterSets, Sessionizer.PAGE_PARAMETER_SET,
                Sessionizer.NAME, parameterSetName);

        if (node == null) {
            return null;
        }

        NodeList nl = node.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo(Sessionizer.PAGE_PARAMETER) == 0) {
                Node n = nl.item(i);
                NamedNodeMap nmAttrs = n.getAttributes();
                PageParserPageParameter pageParameter = new PageParserPageParameter();

                pageParameter.setParameterName(XMLHelper.getAttributeAsString(nmAttrs, Sessionizer.NAME, null));
                pageParameter.setParameterRequired(XMLHelper
                        .getAttributeAsBoolean(nmAttrs, Sessionizer.REQUIRED, false));
                pageParameter.setParameterValue(XMLHelper.getAttributeAsString(nmAttrs, Sessionizer.VALUE, null));
                pageParameter.setRemoveParameterValue(XMLHelper.getAttributeAsBoolean(nmAttrs,
                        Sessionizer.REMOVE_VALUE, false));
                pageParameter.setRemoveParameter(XMLHelper.getAttributeAsBoolean(nmAttrs, Sessionizer.REMOVE_PARAMETER,
                        false));
                pageParameter.setValueSeperator(XMLHelper.getAttributeAsString(nmAttrs, Sessionizer.SEPERATOR, null));

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
            if (nl.item(i).getNodeName().compareTo(Sessionizer.PAGE_DEFINITION) == 0) {
                Node n = nl.item(i);
                PageParserPageDefinition pDef = new PageParserPageDefinition();

                pDef.setWeight(XMLHelper.getAttributeAsInt(n.getAttributes(), Sessionizer.PRIORITY, 0));
                pDef.setProtocol(XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.PROTOCOL, "*"));
                pDef.setHostName(XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.HOSTNAME, "*"));
                pDef.setDirectory(XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.DIRECTORY, null));
                pDef.setMethod(XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.METHOD, "*"));
                pDef.setTemplate(XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.TEMPLATE, null));
                pDef.setValidStatus(XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.STATUSCODES, null));
                pDef.setValidPage(XMLHelper.getAttributeAsBoolean(n.getAttributes(), Sessionizer.VALID, true));
                pDef.setID(XMLHelper.getAttributeAsInt(n.getAttributes(), Sessionizer.ID, 0));

                String parameterSetName = XMLHelper.getAttributeAsString(n.getAttributes(),
                        Sessionizer.PAGE_PARAMETER_SET, null);

                pDef.setValidPageParameters(this.createPageParameters(parameterSetName));

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
        srcSessionDefinition.PeakSessionsAnHour = this.miPeakSessionsAnHour;

        // set main webserver type
        srcSessionDefinition.WebServerType = this.miWebServerType;

        // set default timeout
        srcSessionDefinition.TimeOut = this.miTimeOut;

        NodeList nl = this.mnIdentifiers.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeName().compareTo(Sessionizer.IDENTIFIER_SET) == 0) {
                Node n = nl.item(i);
                NodeList nlIdentifiers = n.getChildNodes();

                String type = XMLHelper.getAttributeAsString(n.getAttributes(), Sessionizer.TYPE, null);
                boolean enableFallBack = XMLHelper.getAttributeAsBoolean(n.getAttributes(), Sessionizer.ENABLEFALLBACK,
                        false);

                boolean expireWhenBetterMatch = XMLHelper.getAttributeAsBoolean(n.getAttributes(),
                        Sessionizer.EXPIREWHENBETTERMATCH, false);

                int priority = XMLHelper.getAttributeAsInt(n.getAttributes(), Sessionizer.PRIORITY, 0);
                int timeOut = XMLHelper.getAttributeAsInt(n.getAttributes(), Sessionizer.TIMEOUT,
                        srcSessionDefinition.TimeOut);

                int DestinationObjectType = -1;

                if (type.equalsIgnoreCase(Sessionizer.MAIN)) {
                    if ((srcSessionDefinition.MainIdentifierTimeOut == 0)) {
                        srcSessionDefinition.MainIdentifierTimeOut = timeOut;
                    }

                    srcSessionDefinition.MainIdentifierFallbackEnabled = enableFallBack;
                    srcSessionDefinition.MainIdentifierExpireWhenBetterMatch = expireWhenBetterMatch;
                    DestinationObjectType = EngineConstants.SESSION_MAIN_SESSION_IDENTIFIER;
                }
                else if (type.equalsIgnoreCase(Sessionizer.IP_BROWSER)) {
                    if (srcSessionDefinition.IPBrowserTimeOut == 0) {
                        srcSessionDefinition.IPBrowserTimeOut = timeOut;
                    }

                    srcSessionDefinition.IPBrowserFallbackEnabled = enableFallBack;
                    srcSessionDefinition.IPBrowserExpireWhenBetterMatch = expireWhenBetterMatch;
                    DestinationObjectType = -1;
                }
                else if (type.equalsIgnoreCase(Sessionizer.PERSISTANT)) {
                    if (srcSessionDefinition.PersistantIdentifierTimeOut == 0) {
                        srcSessionDefinition.PersistantIdentifierTimeOut = timeOut;
                    }

                    srcSessionDefinition.PersistantIdentifierFallbackEnabled = enableFallBack;
                    srcSessionDefinition.PersistantIdentifierExpireWhenBetterMatch = expireWhenBetterMatch;

                    DestinationObjectType = EngineConstants.SESSION_PERSISTANT_IDENTIFIER;
                }
                else if (type.equalsIgnoreCase(Sessionizer.FIRST_CLICK)) {
                    if (srcSessionDefinition.FirstClickIdentifierTimeOut == 0) {
                        srcSessionDefinition.FirstClickIdentifierTimeOut = timeOut;
                    }

                    srcSessionDefinition.FirstClickIdentifierExpireWhenBetterMatch = expireWhenBetterMatch;
                    srcSessionDefinition.FirstClickIdentifierFallbackEnabled = enableFallBack;
                    DestinationObjectType = EngineConstants.SESSION_FIRST_CLICK_IDENTIFIER;
                }
                else if (type.equalsIgnoreCase(Sessionizer.KEEP_VARIABLE)) {
                    DestinationObjectType = EngineConstants.KEEP_COOKIE_VARIABLE;
                }

                for (int idx = 0; idx < nlIdentifiers.getLength(); idx++) {
                    if (nlIdentifiers.item(idx).getNodeName().compareTo(Sessionizer.IDENTIFIER) == 0) {
                        SessionIdentifier sId = new SessionIdentifier();
                        Node nlIdentifier = nlIdentifiers.item(idx);
                        String objectType = XMLHelper.getAttributeAsString(nlIdentifier.getAttributes(),
                                Sessionizer.OBJECTTYPE, null);
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
                                Sessionizer.VARIABLENAME, null), XMLHelper.getAttributeAsBoolean(nlIdentifier
                                .getAttributes(), Sessionizer.CASESENSITIVE, false));

                        sId.Weight = priority;
                        
                        for (int x = 0; x < this.mItemMap.length; x++) {
                            if (this.mItemMap[x] == sId.ObjectType)
                                sId.addSessionIdentifierMap(x);
                        }
                        srcSessionDefinition.addSessionIdentifier(sId);
                    }
                }
            }
        }

        return srcSessionDefinition;
    }

    protected AnalyzePageview restartJob() throws KETLThreadException {
        AnalyzePageview res = null;
        FileInputStream in;

        try {
            in = new FileInputStream(this.getRestartFilenaname());
        } catch (FileNotFoundException e) {
            return null;
        }

        ObjectInputStream s;

        try {
            s = new ObjectInputStream(in);
            res = (AnalyzePageview) s.readObject();
        } catch (Exception e) {
            throw new KETLThreadException("Restart file contains errors, file will be ignored " + e.toString(), this);
        }

        return res;
    }

    protected String getRestartFilenaname() {

        return EngineConstants.CACHE_PATH + File.separator + "KETL.Sessionizer." + this.getName() + "."
                + this.getJobExecutor().getCurrentETLJob().getJobID() + ".restart";
    }

    class SessionizerETLInPort extends ETLInPort {

        int miObjectType;

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            this.miObjectType = EngineConstants.resolveObjectNameToID(XMLHelper.getAttributeAsString(xmlConfig
                    .getAttributes(), Sessionizer.OBJECTTYPE, null));

            return 0;
        }

        public SessionizerETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new SessionizerETLOutPort(this, srcStep);
    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new SessionizerETLInPort(this, srcStep);
    }

    public static String FORMAT_STRING = "FORMATSTRING";
    private static int SESSION = 0;
    private static int HIT = 1;
    private boolean mbHitsNeeded = false;
    private StringBuffer sBuf;

    class SessionizerETLOutPort extends ETLOutPort {

        private static final String DATATYPE = "DATATYPE";
        private static final String SOURCE = "SOURCE";
        int miType = -1;
        int miSource = -1;

        @Override
        public ETLPort getAssociatedInPort() throws KETLThreadException {

            if (this.mObjectType != null)
                return this;

            return super.getAssociatedInPort();
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {

            super.initialize(xmlConfig);

            String type = this.getChannel();

            if (type.equalsIgnoreCase("SESSION")) {
                if (this.mObjectType != null) {
                    this.miSource = Arrays.searchArray(Sessionizer.ValidSessionColumnNames, this.mObjectType);
                    this.getXMLConfig().setAttribute("DATATYPE",
                            Sessionizer.ValidSessionColumnTypes[this.miSource].getCanonicalName());
                }
                this.miType = Sessionizer.SESSION;
                if (this.miSource == Sessionizer.HITS)
                    Sessionizer.this.mbHitsNeeded = true;
            }
            else if (type.equalsIgnoreCase("HIT")) {
                if (this.mObjectType != null) {
                    this.miSource = Arrays.searchArray(Sessionizer.ValidHitColumnNames, this.mObjectType);
                    this.getXMLConfig().setAttribute("DATATYPE",
                            Sessionizer.ValidHitColumnTypes[this.miSource].getCanonicalName());
                }
                this.miType = Sessionizer.HIT;

                if (this.miSource == Sessionizer.ASSOCIATED_HITS)
                    Sessionizer.this.mbHitsNeeded = true;
            }
            else
                throw new KETLThreadException("Channel must be HIT or SESSION", this);

            return 0;
        }

        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            if (this.miType == Sessionizer.SESSION && this.miSource == -1)
                throw new KETLThreadException(
                        "SESSION channel can only contain attributes with a valid OBJECTTYPE attribute", this);

            if (this.miSource == -1 || this.isConstant() || this.isUsed() == false)
                return super.generateCode(portReferenceIndex);

            return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = (("
                    + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).getValue(" + this.miType + ","
                    + this.miSource + ")";

        }

        final public void setDataTypeFromPort(ETLPort in) throws KETLThreadException, ClassNotFoundException {
            if (this.miSource != -1) {
                if (this.miType == Sessionizer.SESSION)
                    ((Element) this.getXMLConfig()).setAttribute(SessionizerETLOutPort.DATATYPE,
                            Sessionizer.ValidSessionColumnTypes[this.miSource].getCanonicalName());
                else if (this.miType == Sessionizer.HIT)
                    ((Element) this.getXMLConfig()).setAttribute(SessionizerETLOutPort.DATATYPE,
                            Sessionizer.ValidHitColumnTypes[this.miSource].getCanonicalName());

                this.setPortClass();
            }
            else
                super.setDataTypeFromPort(in);
        }

        public SessionizerETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        @Override
        public boolean containsCode() throws KETLThreadException {
            if (this.mObjectType != null)
                return true;
            return super.containsCode();
        }

        @Override
        public String getPortName() throws DOMException, KETLThreadException {
            if (this.mstrName == null && this.mObjectType != null) {
                this.mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
                        ETLPort.NAME_ATTRIB, this.mObjectType);
                this.getXMLConfig().setAttribute("NAME", this.mstrName);
            }

            return super.getPortName();
        }

    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        return super.getRecordExecuteMethodHeader() + "if(pOutPath==0) ((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).loadValue(pInputRecords);";
    }

    private boolean skipRecord = false;

    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return ((" + this.getClass().getCanonicalName() + ")this.getOwner()).recordType();}";
    }


    public static final int TEMP_SESSION_ID = 0;

    // hit specific
    public static final int ACTIVITY_DT = 1;
    public static final int GET_REQUEST = 2;
    public static final int STATUS = 3;
    public static final int REFERRER_URL = 4;
    public static final int CLEANSED = 5;
    public static final int CLEANSED_ID = 6;
    public static final int SERVER_NAME = 7;
    public static final int PAGE_SEQUENCE = 8;
    public static final int ASSOCIATED_HITS = 9;

    // session specific
    public static final int FIRST_CLICK_SESSION_IDENTIFIER = 1;
    public static final int PERSISTANT_IDENTIFIER = 2;
    public static final int MAIN_SESSION_IDENTIFIER = 3;
    public static final int IP_ADDRESS = 4;
    public static final int REFERRER = 5;
    public static final int FIRST_SESSION_ACTIVITY = 6;
    public static final int LAST_SESSION_ACTIVITY = 7;
    public static final int BROWSER = 8;
    public static final int REPEAT_VISITOR = 9;
    public static final int HITS = 10;
    public static final int PAGEVIEWS = 11;
    public static final int KEEP_VARIABLES = 12;
    public static final int START_PERSISTANT_IDENTIFIER = 13;
    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.sessionizer.DatabaseWriterRoot#resolveColumnMaps()
     */
    public static final String[] ValidHitColumnNames = { "TEMP_SESSION_ID", "ACTIVITY_DATE_TIME", "GET_REQUEST",
            "HTML_ERROR_CODE", "REFERRER_URL", "CLEANSED", "CLEANSED_ID", "SERVER_NAME", "PAGE_SEQUENCE",
            "ASSOCIATED_HITS" };

    public static final Class[] ValidHitColumnTypes = { Long.class, java.util.Date.class, String.class, Short.class,
            String.class, Short.class, Integer.class, Integer.class, String.class, Short.class };

    public static final String[] ValidSessionColumnNames = { "TEMP_SESSION_ID", "FIRST_CLICK_SESSION_IDENTIFIER",
            "PERSISTANT_IDENTIFIER", "MAIN_SESSION_IDENTIFIER", "IP_ADDRESS", "REFERRER", "FIRST_SESSION_ACTIVITY",
            "LAST_SESSION_ACTIVITY", "BROWSER", "REPEAT_VISITOR", "HITS", "PAGEVIEWS", "KEEP_VARIABLES",
            "START_PERSISTANT_IDENTIFIER" };

    public static final Class[] ValidSessionColumnTypes = { Long.class, String.class, String.class, String.class,
            String.class, String.class, java.util.Date.class, java.util.Date.class, String.class, Boolean.class,
            Short.class, Short.class, String.class, String.class, String.class };
    private AnalyzePageview mAnalyzePageview;
    private List<Session> mCompleteSessionList;

    Session session = null;
    Object[] pageview = null;
    Holder currentPageHolder, pageHolder = AnalyzePageview.newHolder();
    private int[] mItemMap;
    private int defaultReturnType = ETLSplitCore.SUCCESS;
    private boolean returnRecord = false;
    public int recordType() {
        
        if(this.returnRecord){
            this.returnRecord  = false;
            return ETLSplitCore.SUCCESS;            
        }
        
        if (this.skipRecord) {
            this.skipRecord = false;
            return ETLSplitCore.SKIP_RECORD;
        }
        return defaultReturnType;
    }

    public Object getValue(int channel, int source) throws KETLTransformException {
    
        if (channel == Sessionizer.HIT) {
            if (this.currentPageHolder == null)
                throw new KETLTransformException("Session was null");
    
            switch (source) {
            case TEMP_SESSION_ID:
                return this.currentPageHolder.currentSession.ID;
            case ACTIVITY_DT:
                return this.currentPageHolder.pageView[this.mItemMap[Sessionizer.ACTIVITY_DT]];
            case GET_REQUEST:
                return this.currentPageHolder.pageView[this.mItemMap[Sessionizer.GET_REQUEST]];
            case STATUS:
                return this.currentPageHolder.pageView[this.mItemMap[Sessionizer.STATUS]];
            case REFERRER_URL:
                return this.currentPageHolder.pageView[this.mItemMap[Sessionizer.REFERRER_URL]];
            case CLEANSED:
                return this.currentPageHolder.bCleansed;
            case CLEANSED_ID:
                return this.currentPageHolder.iCleansedID;
            case PAGE_SEQUENCE:
                return this.currentPageHolder.iPageSequence;
            case ASSOCIATED_HITS:
                return this.currentPageHolder.iAssociatedHits;
    
            }
        }
        else if (channel == Sessionizer.SESSION) {
    
            int pendingSessions = this.mCompleteSessionList.size();
            if (pendingSessions == 0) {
                this.skipRecord = true;
                return null;
            } 
            
            returnRecord = true;
            
            Session res = this.mCompleteSessionList.remove(0);
    
            switch (source) {
            case TEMP_SESSION_ID:
                return res.ID;
            case FIRST_CLICK_SESSION_IDENTIFIER:
                return res.FirstClickSessionIdentifier;
            case PERSISTANT_IDENTIFIER:
                return res.PersistantIdentifier;
            case MAIN_SESSION_IDENTIFIER:
                return res.MainSessionIdentifier;
            case IP_ADDRESS:
                return res.IPAddress;
            case REFERRER:
                return res.Referrer;
            case FIRST_SESSION_ACTIVITY:
                return res.FirstActivity;
            case LAST_SESSION_ACTIVITY:
                return res.LastActivity;
            case BROWSER:
                return res.Browser;
            case REPEAT_VISITOR:
                return res.RepeatVisitor;
            case HITS:
                return res.Hit;
            case PAGEVIEWS:
                return res.PageViews;
            case KEEP_VARIABLES:
                if (this.sBuf == null) {
                    this.sBuf = new StringBuffer(EngineConstants.MAX_KEEP_VARIABLE_LENGTH);
                }
                else {
                    this.sBuf.delete(0, EngineConstants.MAX_KEEP_VARIABLE_LENGTH - 1);
                }
    
                for (int i = res.CookieKeepVariables.length - 1; i >= 0; i--) {
                    this.sBuf.append(res.CookieKeepVariables[i][0]).append('=').append(res.CookieKeepVariables[i][1])
                            .append(';');
                }
                return this.sBuf.toString();
            case START_PERSISTANT_IDENTIFIER:
                return res.StartPersistantIdentifier;
            }
        }
    
        throw new KETLTransformException("Invalid channel type");
    
    }

    private boolean skipPageScan = false;

    public void loadValue(Object[] data) throws KETLTransformException {

        if (skipPageScan) {
            return;
        }
        
        this.pageHolder.isPageView = false;
        this.pageHolder.currentSession = null;
        this.pageHolder.pageView = data;

        try {
            this.currentPageHolder = this.mAnalyzePageview.analyzeHit(this.pageHolder);
        } catch (Exception e) {
            throw new KETLTransformException(e);
        }

    }

    @Override
    protected void close(boolean success) {

    }

    @Override
    protected boolean remainingRecords() {

        skipPageScan = true;
        defaultReturnType = ETLSplitCore.SKIP_RECORD;
        
        if (this.mRestartSessions) {
            this.mAnalyzePageview.close(false);
        }
        return this.mCompleteSessionList.size() > 0;
    }

}