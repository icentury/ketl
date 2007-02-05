/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ETLJob;
import com.kni.etl.EngineConstants;
import com.kni.etl.ParameterList;
import com.kni.etl.SharedCounter;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.qa.QACollection;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public abstract class ETLStep extends ETLWorker {

    private ETLJob mJob;

    @Override
    final protected void interruptExecution() throws InterruptedException {
        if (this.mJob.isKilled()) {
            throw new InterruptedException("Job has been killed");
        }
        else if (this.mJob.isPaused()) {
            setWaiting("Pause to be released");
            while (this.mJob.isPaused()) {
                Thread.sleep(1000);
            }
            setWaiting(null);
            this.interruptExecution();
        }
    }

    final static String[] TAGS_NOT_SUPPORTING_PARAMETERS = { "FILTER", "OUT" };

    public static final String BATCHSIZE_ATTRIB = "BATCHSIZE";

    public static final String CASE_TAG = "CASE";

    public static final String DEFAULT_TAG = "DEFAULT";

    public static final String DRIVING_STEP_ATTRIB = "DRIVING_STEP";

    public static final String ERRORLIMIT_ATTRIB = "ERRORLIMIT";
    public static final String FATAL_ERROR_HANDLER = "FATAL_ERROR";

    public static final String IN_TAG = "IN";

    public static final String LOG_BAD_RECORDS = "LOGBADRECORDS";

    public static final String LOG_ERROR_HANDLER = "LOG_ERROR";

    public static final String LOG_MESSAGE_HANDLER = "LOG_MESSAGE";

    public static final String NAME_ATTRIB = "NAME";

    public static final String OUT_TAG = "OUT";

    public static final String SHOWEXCEPTIONS = "SHOWEXCEPTIONS";

    public static final String SQL_ATTRIB = "SQL";

    public static final String TRIGGER_TAG = "TRIGGER";

    public static final String XMLSOURCE_ATTRIB = "XMLSOURCE";

    public static final String XMLSOURCENAME_ATTRIB = "XMLSOURCENAME";
    protected List maParameters;
    protected KETLJobExecutor mkjExecutor = null;
    private HashMap mStepTemplate = new HashMap();

    /**
     * @param pXMLConfig TODO
     * @throws KETLThreadException TODO
     */
    public ETLStep(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        String strParameterListName = null;

        // Find the name of the parameter list to be used...
        if ((strParameterListName = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(),
                EngineConstants.PARAMETER_LIST, null)) == null
                && this.getRequiredTags() != null && this.getRequiredTags().length > 0) {
            throw new KETLThreadException("Missing required parameters: " + this.getRequiredTagsMessage(), this);
        }

        /*
         * get parameter list values, this will parse all parameter lists and populate maParameters with lists of
         * complete parameters.
         */
        if (strParameterListName != null)
            if (getParamaterLists(strParameterListName) != 0) {
                throw new KETLThreadException("No complete parameter sets found, check that the following exist:\n"
                        + getRequiredTagsMessage(), this);
            }

    }

    private OutputStream moDump, moDumpBuffer;
    private Writer mDumpWriter;
    private String mDumpFile;
    private boolean mLoggerFailed = false;

    protected void logBadRecord(int pRowNum, Object[] pRec, Exception e2) throws IOException {
        if (this.mLoggerFailed)
            return;
        try {
            if (moDump == null) {
                mDumpFile = EngineConstants.BAD_RECORD_PATH + File.separator
                        + this.getJobExecutor().getCurrentETLJob().getJobID() + "."
                        + this.getJobExecutor().getCurrentETLJob().getJobExecutionID();
                moDump = new FileOutputStream(mDumpFile);
                moDumpBuffer = new BufferedOutputStream(moDump);
                mDumpWriter = new PrintWriter(moDumpBuffer);

                if (this.mInPorts != null) {
                    mDumpWriter.write("Input record format(Constants excluded)\n");
                    Object inCols[] = new Object[1024];
                    for (int i = 0; i < this.mInPorts.length; i++) {
                        if (this.mInPorts[i].isConstant() == false)
                            inCols[this.mInPorts[i].getSourcePortIndex()] = this.mInPorts[i].mstrName;
                    }
                    for (int i = 0; i < inCols.length; i++) {
                        if (inCols[i] != null)
                            mDumpWriter.write(inCols[i] + "|");
                    }
                    mDumpWriter.write("\n");
                }
                if (this.mOutPorts != null) {
                    mDumpWriter.write("Output record format(Constants excluded)\n");
                    Object outCols[] = new Object[1024];

                    for (int i = 0; i < this.mOutPorts.length; i++) {
                        if (this.mOutPorts[i].isConstant() == false) {
                            try {
                                outCols[this.mOutPorts[i].getPortIndex()] = this.mOutPorts[i].mstrName;
                            } catch (Exception e) {
                                // TODO: Wrong column mapping, review code
                                outCols[i] = "Error resolving column name";
                            }
                        }
                    }
                    for (int i = 0; i < outCols.length; i++) {
                        if (outCols[i] != null)
                            mDumpWriter.write(outCols[i] + "|");
                    }
                    mDumpWriter.write("\n");
                }

            }
            mDumpWriter.write("Row: " + pRowNum + "|");
            for (int i = 0; i < pRec.length; i++) {
                mDumpWriter.write(pRec[i] == null ? "[NULL]|" : pRec[i].toString().replace("|", "\\|") + "|");
            }

            mDumpWriter.write("|" + e2.toString() == null ? "[NULL]" : e2.toString().replace("|", "\\|").replace("\n",
                    " ")
                    + "\n");

        } catch (IOException e) {
            this.mLoggerFailed = true;
            ResourcePool.LogMessage(e, ResourcePool.ERROR_MESSAGE, "Bad record logging failed: " + e.toString());
            ResourcePool.LogException(e, this);
        }

    }

    @Override
    public void closeStep(boolean success) {
        super.closeStep(success);
        if (this.mLoggerFailed == false && this.moDump != null) {
            try {
                this.mDumpWriter.flush();
                this.mDumpWriter.close();
                this.moDumpBuffer.close();
                this.moDump.close();
            } catch (IOException e) {
                this.mLoggerFailed = true;
                ResourcePool.LogMessage(e, ResourcePool.ERROR_MESSAGE, "Bad record logging failed: " + e.toString());
                ResourcePool.LogException(e, this);
            }
        }
    }

    /**
     * @return
     */
    public KETLJobExecutor getJobExecutor() {
        return this.getThreadManager().getJobExecutor();
    }

    protected String getMethodMapFromSystemXML(String pMethod, Class pClass, Class pRequiredDatatype,
            String errorMessage) throws KETLThreadException {
        throw new KETLThreadException(errorMessage, this);
    }

    protected int getParamaterLists(String strParameterListName) {
        ArrayList tmp = this.getJobExecutor().getCurrentETLJob().getParameterLists(strParameterListName);
        ArrayList res = new ArrayList();

        for (Object o : tmp) {
            if (this.hasCompleteParameterSet((com.kni.etl.ParameterList) o)) {
                res.add(o);
            }
        }

        if (res.size() == 0)
            return 5;

        this.maParameters = res;
        return 0;

    }

    public String getParameterValue(int iParamList, String strParamName) {
        if (this.maParameters != null) {
            ParameterList paramList = (ParameterList) this.maParameters.get(iParamList);

            if (paramList == null) {
                return null;
            }

            String val = (String) paramList.getParameter(strParamName);
            if (val != null && this.getJobExecutor() != null && this.getJobExecutor().getCurrentETLJob() != null) {
                val = this.getJobExecutor().getCurrentETLJob().getInternalConstants(val);
            }

            return val;
        }

        return null;
    }

    public String getQAClass(String strQAType) {
        // TODO Auto-generated method stub
        return null;
    }

    protected String[] getRequiredTags() {

        Node n = XMLHelper.findElementByName(EngineConstants.getSystemXML(), "STEP", "CLASS", this.getClass()
                .getCanonicalName());

        if (n == null)
            throw new RuntimeException(
                    "Class requires an entry in the System.xml file to be valid, please add an entry for class "
                            + this.getClass().getCanonicalName());
        NodeList nl = ((Element) n).getElementsByTagName("PARAMETERS");

        HashSet res = new HashSet();
        for (int i = 0; i < nl.getLength(); i++) {
            Node[] params = XMLHelper.getElementsByName(nl.item(i), "PARAMETER", "REQUIRED", "TRUE");

            if (params != null)
                for (int x = 0; x < params.length; x++)
                    res.add(((Element) params[x]).getAttribute("NAME"));
        }

        Class cl = this.getClass().getSuperclass();

        if (cl != null && ETLWorker.class.isAssignableFrom(cl)) {
            this.getRequiredTags(res, cl);
        }

        String[] result = new String[res.size()];
        res.toArray(result);

        return result;
    }

    private void getRequiredTags(HashSet parentList, Class requestedClass) {

        Node n = XMLHelper.findElementByName(EngineConstants.getSystemXML(), "STEP", "CLASS", requestedClass
                .getCanonicalName());

        if (n == null)
            return;

        NodeList nl = ((Element) n).getElementsByTagName("PARAMETERS");

        for (int i = 0; i < nl.getLength(); i++) {
            Node[] params = XMLHelper.getElementsByName(nl.item(i), "PARAMETER", "REQUIRED", "TRUE");

            if (params != null)
                for (int x = 0; x < params.length; x++) {
                    if (parentList.contains(params) == false)
                        parentList.add(((Element) params[x]).getAttribute("NAME"));
                }
        }

        Class cl = requestedClass.getSuperclass();
        if (cl != null && ETLWorker.class.isAssignableFrom(cl)) {
            this.getRequiredTags(parentList, cl);
        }

    }

    protected String getRequiredTagsMessage() {
        if (getRequiredTags() == null) {
            return "Step is missing getRequiredTags(), coding error, please report bug";
        }

        String msg = "";

        for (int i = 0; i < getRequiredTags().length; i++) {
            String str = getRequiredTags()[i];
            msg = msg + "\t" + str + "\n";
        }

        return msg;
    }

    protected final String getStepTemplate(String pGroup, String pName, boolean pDefaultAllowed)
            throws KETLThreadException {
        return this.getStepTemplate(this.getClass(), pGroup, pName, pDefaultAllowed);
    }

    protected final String getStepTemplate(Class parentClass, String pGroup, String pName, boolean pDefaultAllowed)
            throws KETLThreadException {
        Element template = this.getStepTemplates(parentClass);

        if (template == null) {
            throw new KETLThreadException("Template missing from system file CLASS="
                    + this.getClass().getCanonicalName() + " GROUP=" + pGroup + " NAME=" + pName, this);
        }

        Class superCl = this.getClass().getSuperclass();

        synchronized (template) {
            // get group
            Element e = (Element) XMLHelper.getElementByName(template, "GROUP", "NAME", pGroup);

            // if group is null then try default group
            if (e == null) {

                if (pDefaultAllowed)
                    e = (Element) XMLHelper.getElementByName(template, "GROUP", "NAME", "DEFAULT");

                // if still null then go to parent parent class
                if (e == null) {
                    if (superCl != null && ETLWorker.class.isAssignableFrom(superCl))
                        return this.getStepTemplate(superCl, pGroup, pName, pDefaultAllowed);

                    throw new KETLThreadException("Template group \"" + pGroup + "\" not found", this);
                }
            }

            e = (Element) XMLHelper.getElementByName(e, "TEMPLATE", "NAME", pName);

            if (e == null) {

                // if not found in main group then go back to default
                if (pDefaultAllowed) {
                    e = (Element) XMLHelper.getElementByName(template, "GROUP", "NAME", "DEFAULT");

                    // go to parent
                    if (e != null) {
                        e = (Element) XMLHelper.getElementByName(e, "TEMPLATE", "NAME", pName);
                    }
                }

                // go to parent
                if (e == null) {
                    if (superCl != null && ETLWorker.class.isAssignableFrom(superCl))
                        return this.getStepTemplate(superCl, pGroup, pName, pDefaultAllowed);
                    throw new KETLThreadException("Template group \"" + pGroup + "\" element \"" + pName
                            + "\" not found", this);
                }

            }

            return XMLHelper.getTextContent(e);

        }
    }

    protected final Element getStepTemplates(Class pClass) throws KETLThreadException {
        Document doc = EngineConstants.getSystemXML();

        if (doc == null)
            throw new KETLThreadException("System.xml cannot be found or instantiated", this);

        Element node = (Element) this.mStepTemplate.get(pClass.getCanonicalName());
        if (node == null) {
            synchronized (doc) {
                node = (Element) XMLHelper.findElementByName(doc, "STEP", "CLASS", pClass.getCanonicalName());
                if (node != null) {
                    node = (Element) XMLHelper.getElementByName(node, "TEMPLATES", null, null);
                    this.mStepTemplate.put(pClass.getCanonicalName(), node);
                }
            }
        }

        return node;
    }

    private boolean hasCompleteParameterSet(com.kni.etl.ParameterList aParametersAndValues) {
        if (getRequiredTags() == null) {
            return true;
        }

        for (int i = 0; i < getRequiredTags().length; i++) {
            boolean found = false;

            if (aParametersAndValues.getParameter(getRequiredTags()[i]) != null) {
                found = true;
            }

            if (found == false) {
                return false;
            }
        }

        return true;
    }

    protected static int DEFAULT_BATCHSIZE = 1000;
    protected boolean mbLogBadRecords = false, mShowExceptions = false;
    protected static int DEFAULT_ERRORLIMIT = 0;
    private int miErrorLimit = 0;

    private QACollection mqacQACollection;

    private List mvTriggers = new ArrayList();

    @Override
    public int complete() throws KETLThreadException {
        this.mbLastThreadToComplete = this._isLastThreadToComplete();

        if (this.isLastThreadToEnterCompletePhase())
            this.mqacQACollection.completeCheck();

        return super.complete();
    }

    final protected void recordCheck(Object[] di, Exception e) throws KETLQAException {
        this.mqacQACollection.recordCheck(di, e);
        this.mqacQACollection.itemChecks(di, e);
    }

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {

        // Get child nodes...
        try {
            NodeList nl = xmlConfig.getChildNodes();

            for (int i = 0; i < nl.getLength(); i++) {
                Node node = nl.item(i);

                if (node.getNodeName().compareTo(TRIGGER_TAG) == 0) {
                    if (addTrigger(node) == null) {
                        return -4;
                    }
                }
                else {
                    // Add other children as needed
                }
            }
        } catch (Exception e) {
            ResourcePool.LogMessage(this, e.getMessage());
            return -1;
        }
        
        this.mbFirstThreadToStart = this._isFirstThreadToStart();
        // initialize any qa for this step
        this.mqacQACollection = getQACollection(xmlConfig);

        this.getStepTemplates(this.getClass());

        batchSize = XMLHelper.getAttributeAsInt(xmlConfig.getParentNode().getParentNode().getAttributes(),
                BATCHSIZE_ATTRIB, DEFAULT_BATCHSIZE);

        mbLogBadRecords = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), LOG_BAD_RECORDS,
                this.mbLogBadRecords);

        mShowExceptions = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), SHOWEXCEPTIONS, mShowExceptions);

        // Pull the error limit...
        miErrorLimit = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), ERRORLIMIT_ATTRIB, DEFAULT_ERRORLIMIT);

        /*
         * String strParameterListName = null; // Find the name of the parameter list to be used... if
         * ((strParameterListName = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
         * EngineConstants.PARAMETER_LIST, null)) == null && this.getRequiredTags() != null &&
         * this.getRequiredTags().length > 0) { throw new KETLThreadException("Missing required parameters: " +
         * this.getRequiredTagsMessage()); } get parameter list values, this will parse all parameter lists and populate
         * maParameters with lists of complete parameters. if (strParameterListName != null) if
         * (getParamaterLists(strParameterListName) != 0) { ResourcePool.LogMessage(this, "No complete parameter sets
         * found, check that the following exist:\n" + getRequiredTagsMessage()); return 4; }
         */
        this.mErrorCounter = this.getJobExecutor().ejCurrentJob.getErrorCounter(this.getName());

      

        if (this.isFirstThreadToEnterInitializePhase())
            this.mqacQACollection.initializeCheck();

        return 0;
    }

    protected ETLTrigger addTrigger(Node xmlNode) throws KETLThreadException {
        ETLTrigger tTrigger = new ETLTrigger(this);

        // Call the initialize() method ourselves to get any errors in the config...
        if (tTrigger.initialize(xmlNode, this) != 0) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "unable to create trigger in step '"
                    + this.getName() + "'.");

            return null;
        }

        mvTriggers.add(tTrigger);

        return tTrigger;
    }

    private QACollection getQACollection(Node xmlConfig) throws KETLThreadException {
        return this.getJobExecutor().getQACollection(this.getName(), this, xmlConfig);
    }

    protected QACollection getQACollection() throws KETLThreadException {

        if (this.mqacQACollection == null)
            this.mqacQACollection = this.getQACollection(this.getXMLConfig());
        return this.mqacQACollection;
    }

    public void recordToLog(Object entry, boolean info) {

        if (this.getJobExecutor() == null || this.getJobExecutor().ejCurrentJob == null)
            return;

        java.util.Date dt = new java.util.Date();
        ArrayList log = (ArrayList) this.getJobExecutor().ejCurrentJob.getLog(this.getName());
        this.getJobExecutor().ejCurrentJob.logJobMessage("[" + dt.toString() + "]" + entry);
        if (info)
            return;
        // cannot have more than a 100 in memory
        if (log.size() > 100) {
            log.remove(0);
        }

        if (log.contains(entry) == false) {
            log.add(new Object[] { entry, dt });
        }
    }

    public ArrayList getLog() {
        return (ArrayList) this.getJobExecutor().ejCurrentJob.getLog(this.getName());
    }

    /**
     * @param executor
     */
    public void setJobExecutor(KETLJobExecutor executor) {
        mkjExecutor = executor;
        this.mJob = this.mkjExecutor.ejCurrentJob;
    }

    public boolean showException() {
        return mShowExceptions;
    }

    SharedCounter mErrorCounter = null;

    public int getErrorCount() {
        return this.mErrorCounter.value();
    }

    public Exception getLastException() {
        return this.mLastError;
    }

    private Exception mLastError = null;

    private boolean mbFirstThreadToStart = false;

    private boolean mbLastThreadToComplete = false;

    protected void incrementErrorCount(Exception e, int i, int recordCounter) throws Exception {

        this.mLastError = e;
        if (this.mErrorCounter.increment(i) > this.miErrorLimit) {
            if (this.miErrorLimit == 0) {
                throw e;
            }
            throw e;
        }

    }

    protected void incrementErrorCount(ETLEvent event, int i) throws KETLQAException {

        if (this.mErrorCounter.increment(i) > this.miErrorLimit) {
            throw new KETLQAException("Step halted, QA failed, see below for details: " + event.mstrMessage, event,
                    this);
        }

    }

    @Override
    public boolean success() {

        SharedCounter cnt = this.getJobExecutor().ejCurrentJob.getErrorCounter(this.getName());

        int res = cnt.value();
        if (res > this.miErrorLimit) {
            if (this.getJobExecutor().getCurrentETLJob().getStatus().getException() == null)
                this.mkjExecutor.getCurrentETLJob().getStatus().setException(this.mLastError);
            return false;

        }

        return true;
    }

    private boolean mbCaseCheckPerformedAlready = false;

    protected boolean matchesEventHandler(String strHandler, String strRequiredHandler) {
        if (strHandler.equals(strRequiredHandler)) {
            return true;
        }

        if ((mbCaseCheckPerformedAlready == false) && strHandler.equalsIgnoreCase(strRequiredHandler)) {
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Event handler is \"" + strHandler
                    + "\" and event generated is \"" + strRequiredHandler + "\" possible case error in XML");
            mbCaseCheckPerformedAlready = true;
        }

        return false;
    }

    public int handleEvent(String strHandler, ETLEvent event) throws KETLQAException {
        if (matchesEventHandler(strHandler, LOG_MESSAGE_HANDLER)) {
            return handleLogMessage(event);
        }
        else if (matchesEventHandler(strHandler, FATAL_ERROR_HANDLER)) {
            return handleFatalError(event);
        }
        else if (matchesEventHandler(strHandler, LOG_ERROR_HANDLER)) {
            return handleErrorMessage(event);
        }

        return 0;
    }

    public int handleLogMessage(ETLEvent event) {
        // to DB
        ResourcePool.LogMessage(event.getETLStep(), ResourcePool.EVENT_MESSAGE_TYPE, ResourcePool.INFO_MESSAGE,
                event.mstrMessage, event.getExtendedMessage(), true);

        // to stdout
        ResourcePool.LogMessage(event.getETLStep(), ResourcePool.EVENT_MESSAGE_TYPE, ResourcePool.INFO_MESSAGE,
                event.mstrMessage, event.getExtendedMessage(), false);

        return 1;
    }

    public int handleErrorMessage(ETLEvent event) throws KETLQAException {
        this.incrementErrorCount(event, 1);

        return 1;
    }

    public int handleFatalError(ETLEvent event) throws KETLQAException {
        ResourcePool.LogMessage(event.getETLStep(), ResourcePool.EVENT_MESSAGE_TYPE, ResourcePool.FATAL_MESSAGE,
                event.mstrMessage, event.getExtendedMessage(), true);
        throw new KETLQAException("Step halted, QA failed, see below for details: " + event.mstrMessage, event, this);
    }

    private boolean _isFirstThreadToStart() {
        ETLJob kj = this.mkjExecutor.getCurrentETLJob();
        SharedCounter cnt = kj.getCounter("STARTUP" + this.getName());

        if (cnt.increment(1) == 1)
            return true;

        return false;
    }

    private boolean _isLastThreadToComplete() {

        ETLJob kj = this.mkjExecutor.getCurrentETLJob();

        SharedCounter cnt = kj.getCounter("SHUTDOWN" + this.getName());
        if (cnt.increment(1) == this.partitions) {
            return true;
        }
        return false;
    }

    public boolean isFirstThreadToEnterInitializePhase() {
        return mbFirstThreadToStart;
    }

    public boolean isLastThreadToEnterCompletePhase() {
        return mbLastThreadToComplete;
    }

    public void logException(KETLThreadException exception) {
        this.mLastError = exception;

        this.recordToLog(exception, false);

        if (this.mErrorCounter == null)
            this.mErrorCounter = this.getJobExecutor().ejCurrentJob.getErrorCounter(this.getName());

        this.mErrorCounter.increment(1);
    }

    public List getTriggers() {
        return this.mvTriggers;
    }

    public ETLStep getTargetStep(String mstrTargetStep) throws KETLThreadException {
        return (ETLStep) this.getThreadManager().getStep(this, mstrTargetStep);

    }

}
