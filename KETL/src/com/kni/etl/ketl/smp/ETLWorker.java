/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 5, 2006
 * 
 */
package com.kni.etl.ketl.smp;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.w3c.dom.Attr;
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
import com.kni.etl.ketl.KETLJobExecutor;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.util.ClassFromCode;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
abstract public class ETLWorker implements Runnable {

    class CodeField {

        boolean constant = false;
        String datatype;
        String name;
        boolean privateValue = false;
        String value;

        public String toString() {
            return name;
        }
    }

    final public boolean debug() {
        return this.mDebug;
    }

    public static final int CHANNEL = 1;

    public static final int DEFAULT = 0;
    public final static Object ENDOBJ = new Object();
    public static final int LEFT = 0;
    public static final int PORT = 2;

    public static final int RIGHT = 1;

    static final int STEP = 0;

    protected void configureBufferSort(ManagedBlockingQueue srcQueue) {

        if (srcQueue instanceof Partitioner)
            return;

        Node[] sortKeys = XMLHelper.getElementsByName(this.getXMLConfig(), "IN", "BUFFERSORT", null);
        Comparator comp = null;

        if (sortKeys != null && sortKeys.length > 0) {
            Integer[] elements = new Integer[sortKeys.length];
            Boolean[] elementOrder = new Boolean[sortKeys.length];

            for (int i = 0; i < sortKeys.length; i++) {
                elements[i] = XMLHelper.getAttributeAsInt(sortKeys[i].getAttributes(), "BUFFERSORT", 0);
                elementOrder[i] = XMLHelper.getAttributeAsBoolean(sortKeys[i].getAttributes(), "BUFFERSORTORDER", true);
            }
            comp = new DefaultComparator(elements, elementOrder);

            ((ManagedBlockingQueueImpl) srcQueue).setSortComparator(comp);
        }

    }

    final protected static String[] extractPortDetails(String content) throws KETLThreadException {

        if (content == null)
            return null;

        content = content.trim();

        if (content.startsWith("\"") && content.endsWith("\""))
            return null;

        String[] sources = content.split("\\.");

        if (sources == null || sources.length == 1 || sources.length > 3)
            throw new KETLThreadException("IN port definition invalid: \"" + content + "\"", Thread.currentThread());

        String[] res = new String[3];
        res[STEP] = sources[0];
        if (sources.length == 3) {
            res[CHANNEL] = sources[1];
            res[PORT] = sources[2];

        }
        else
            res[PORT] = sources[1];

        return res;
    }

    final public static String getChannel(Element xmlConfig, int type) throws KETLThreadException {

        Node[] ports;
        if (type == DEFAULT)
            ports = XMLHelper.getElementsByName(xmlConfig, "IN", "*", "*");
        else
            ports = XMLHelper.getElementsByName(xmlConfig, "IN", type == LEFT ? "LEFT" : "RIGHT", "TRUE");

        for (int i = 0; i < ports.length; i++) {
            Node port = ports[i];
            String content = XMLHelper.getTextContent(port);

            if (content == null)
                continue;

            content = content.trim();

            if (content.startsWith("\"") && content.endsWith("\""))
                continue;

            String[] sources = content.split("\\.");

            if (sources == null || sources.length == 1 || sources.length > 3)
                throw new KETLThreadException("IN port definition invalid: \"" + content + "\"", Thread.currentThread());

            if (sources.length == 2)
                return "DEFAULT";

            return sources[1];
        }

        throw new KETLThreadException("Step \""
                + XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NAME", "n/a")
                + "\" has no in ports or ports do not have a valid source", Thread.currentThread());
    }

    final public static String[] getChannels(Element config) {
        NodeList nl = config.getElementsByTagName("OUT");
        HashSet ports = new HashSet();
        for (int i = 0; i < nl.getLength(); i++) {
            ports.add(XMLHelper.getAttributeAsString(nl.item(i).getAttributes(), "CHANNEL", "DEFAULT"));
        }

        String[] res = new String[ports.size()];
        ports.toArray(res);

        return res;
    }

    final public static String[] getSource(Element xmlConfig) throws KETLThreadException {

        String left = null, right = null;
        NodeList nl = xmlConfig.getElementsByTagName("IN");
        for (int i = 0; i < nl.getLength(); i++) {
            boolean leftSource = false, rightSource = false;
            Node port = nl.item(i);

            String content = XMLHelper.getTextContent(port);

            if (ETLPort.containsConstant(content))
                continue;

            if (port.hasAttributes() && XMLHelper.getAttributeAsBoolean(port.getAttributes(), "RIGHT", false))
                rightSource = true;
            else
                leftSource = true;

            String[] sources = extractPortDetails(content);

            if (sources == null)
                continue;

            if (left == null && leftSource)
                left = sources[STEP];
            else if (right == null && rightSource)
                right = sources[STEP];
        }

        if (right == null)
            return new String[] { left };

        return new String[] { left, right };
    }

    final public static void setOutDefaults(Element pConfig) throws KETLThreadException {
        NodeList nl = pConfig.getElementsByTagName("OUT");

        for (int i = 0; i < nl.getLength(); i++) {

            String content = XMLHelper.getTextContent(nl.item(i));

            if (!(content == null || content.trim().equals("*") || content.trim().length() == 0 || content.trim()
                    .startsWith(com.kni.etl.EngineConstants.VARIABLE_PARAMETER_START)
                    && content.trim().endsWith(com.kni.etl.EngineConstants.VARIABLE_PARAMETER_END))
                    && (nl.item(i).hasAttributes() == false))
                throw new KETLThreadException("Invalid out node found in the XML - " + XMLHelper.outputXML(nl.item(i)),
                        Thread.currentThread());

            Element n = (Element) nl.item(i);

            if (n.hasAttribute("CHANNEL") == false)
                n.setAttribute("CHANNEL", "DEFAULT");
        }
    }

    int tuneInterval = 200000;

    private String coreClassName = null;

    private final int defaultBatchSize = 1000;

    private final int defaultQueueSize = 5;

    protected HashMap hmInports = new HashMap();

    protected HashMap hmOutports = new HashMap();

    protected final boolean mBatchManagement = this.implementsBatchManagement();

    HashMap mChannelClassMapping = new HashMap();

    protected HashMap mChannelPortsUsed = new HashMap();

    ArrayList mCodeFields = new ArrayList();

    HashMap mCodeFieldsLookup = new HashMap();

    private HashMap mhmInportIndex = new HashMap();

    protected HashMap mHmOutportIndex = new HashMap();

    protected ETLInPort[] mInPorts;
    protected ETLOutPort[] mOutPorts;
    HashMap mSourceOuts = new HashMap();

    String mstrName;

    private ETLThreadManager mThreadManager;

    protected ETLThreadManager getThreadManager() {
        return this.mThreadManager;
    }

    private int pos = 0;

    protected int queueSize, batchSize, partitionID, partitions;
    private int recordCount = 0;
    private boolean selfTune = true;

    private Node xmlConfig;

    int tuneIntervalIncrement = 200000;

    protected boolean timing;

    protected boolean mDebug;
    protected boolean mMonitor;

    /**
     * @param pPartitionID TODO
     * @throws KETLThreadException
     */
    public ETLWorker(Node pXMLConfig, int pPartitionID, int pPartitions, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super();
        this.queueSize = XMLHelper.getAttributeAsInt(pXMLConfig.getAttributes(), "QUEUESIZE", defaultQueueSize);
        this.batchSize = XMLHelper.getAttributeAsInt(pXMLConfig.getAttributes(), "BATCHSIZE", defaultBatchSize);
        this.timing = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "TIMING", false);

        this.mThreadManager = pThreadManager;
        this.partitionID = pPartitionID;
        this.mstrName = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), "NAME", null);
        this.partitions = pPartitions;
        this.mThreadManager.addStep(this);
        this.xmlConfig = pXMLConfig;
        this.mDebug = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "DEBUG", false);
        this.mMonitor = XMLHelper.getAttributeAsBoolean(pXMLConfig.getAttributes(), "MONITOR", false);

        try {
            Class cl = Class.forName("com.kni.etl.ketl.smp.ETLBatchOptimizer");
            ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Batch optimizer enabled");
            this.tuneIntervalIncrement = XMLHelper.getAttributeAsInt(pXMLConfig.getParentNode().getAttributes(),
                    "AUTOTUNEINCREMENT", this.tuneIntervalIncrement);
            this.mBatchOptimizer = (BatchOptimizer) cl.newInstance();
        } catch (Exception e) {
        }
    }

    final public void compile() throws KETLThreadException {
        Class coreClass = null;
        if (!(this instanceof DefaultCore))
            coreClass = generateCore();
        this.instantiateCore(coreClass);
    }

    public int complete() throws KETLThreadException {
        return 0;
    }

    protected long totalTimeNano = 0;
    protected long startTimeNano = 0;

    abstract protected void executeWorker() throws InterruptedException, ClassNotFoundException, KETLThreadException,
            KETLReadException, IOException, KETLTransformException, KETLWriteException;

    private Class generateCore() throws KETLThreadException {

        StringBuilder sb = new StringBuilder("package job."
                + ((ETLStep) this).getJobExecutor().getCurrentETLJob().getJobID() + ";\n");

        sb.append(generateCoreImports());
        sb.append(generateCoreHeader());

        sb.append(generatePortMappingCode());

        // declare fields and constants
        for (Object o : this.mCodeFields) {
            CodeField cons = (CodeField) o;
            if (cons.privateValue)
                sb.append("private ");

            if (cons.constant)
                sb.append("static final " + cons.datatype + " " + cons.name + " = " + cons.value + ";\n");
            else
                sb.append(cons.datatype + " " + cons.name + ";\n");
        }

        // initialize fields
        sb.append("protected void initializeCoreFields() {");
        for (Object o : this.mCodeFields) {
            CodeField cons = (CodeField) o;
            if (cons.constant == false)
                sb.append(cons.name + " = " + cons.value + ";\n");
        }

        sb.append("}");

        sb.append('}');

        // compile core
        try {
            return ClassFromCode.getDynamicClass(((ETLStep) this).getJobExecutor().getCurrentETLJob(), sb.toString(),
                    this.getCoreClassName(), false, false);
            // (getCoreClassName(), sb.toString());
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }
    }

    abstract protected CharSequence generateCoreHeader();

    protected String generateCoreImports() {
        return "import com.kni.etl.ketl.exceptions.*;\n" + "import com.kni.etl.ketl.smp.*;\n";
    }

    protected String generatePortMappingCode() throws KETLThreadException {
        StringBuilder sb = new StringBuilder();
        // generate constants used for references
        if (this.mInPorts != null) {
            for (int i = 0; i < this.mInPorts.length; i++) {
                if (this.mInPorts[i].isConstant())
                    this.getCodeField(this.mInPorts[i].getPortClass().getCanonicalName(), "new "
                            + this.mInPorts[i].getPortClass().getCanonicalName() + "(\""
                            + this.mInPorts[i].getConstantValue().toString() + "\")", true, true, this.mInPorts[i]
                            .generateReference());
            }
        }
        // generate port maps

        // generate mapping method header;
        sb.append(this.getRecordExecuteMethodHeader() + "\n");
        // outputs
        if (this.mOutPorts != null)
            for (int i = 0; i < this.mOutPorts.length; i++) {

                sb.append("try { " + this.mOutPorts[i].generateCode(i));
                sb.append(";} catch(Exception e) { if(e instanceof " + this.getDefaultExceptionClass() + ") { throw ("
                        + this.getDefaultExceptionClass() + ")e; } else {throw new " + this.getDefaultExceptionClass()
                        + "(\"Port " + this.mOutPorts[i].mstrName + " generated exception \" + e.toString(),e);}}\n");
            }

        // generate mapping method footer
        sb.append(this.getRecordExecuteMethodFooter() + "\n");

        return sb.toString();
    }

    abstract String getDefaultExceptionClass();

    protected BatchManager getBatchManager() {
        if (implementsBatchManagement()) {
            return (BatchManager) this;
        }
        return null;
    }

    protected String getCodeField(String datatype, String value, boolean constant, boolean privateValue, String name) {

        String nm = (name == null ? datatype + constant : name);

        if (mCodeFieldsLookup.containsKey(nm))
            return ((CodeField) mCodeFieldsLookup.get(nm)).name;

        CodeField cons = new CodeField();
        cons.constant = constant;
        cons.datatype = datatype;
        cons.name = (name == null ? "CONST_" + mCodeFieldsLookup.size() : name);
        cons.value = value;
        cons.privateValue = privateValue;
        mCodeFieldsLookup.put(nm, cons);
        mCodeFields.add(cons);

        return cons.name;
    }

    final public String getCodeGenerationOutputObject(String pChannel) {
        return "pOutputRecords";
    }

    final protected String getCoreClassName() {
        if (coreClassName == null)
            coreClassName = this.mstrName;// + this.getJobExecutionID();

        return coreClassName;
    }

    final public ETLInPort getInPort(int index) {
        for (int i = 0; i < this.mInPorts.length; i++)
            if (this.mInPorts[i].getSourcePortIndex() == index)
                return this.mInPorts[i];

        return null;
    }

    final public ETLInPort getInPort(String arg0) {
        return (ETLInPort) this.hmInports.get(arg0);
    }

    final protected long getJobExecutionID() {
        return 1;// System.nanoTime();
    }

    final public String getName() {
        return this.mstrName;
    }

    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new ETLInPort((ETLStep) this, srcStep);
    }

    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new ETLOutPort((ETLStep) this, (ETLStep) this);
    }

    final protected String getOutChannel() {
        return (String) this.mChannelClassMapping.keySet().toArray()[0];
    }

    final public ETLOutPort getOutPort(int index) {
        return this.mOutPorts[index];
    }

    final public ETLOutPort getOutPort(String arg0) throws KETLThreadException {

        ETLOutPort port = (ETLOutPort) this.hmOutports.get(arg0);

        if (port == null) {
            // check for it
            Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "*", "*");
            if (nl != null) {
                for (int i = 0; i < nl.length; i++) {
                    String nm = XMLHelper.getAttributeAsString(nl[i].getAttributes(), "NAME", null);
                    if (nm == null || nm.equals(arg0)) {
                        ETLOutPort newPort = this.getNewOutPort((ETLStep) this);
                        try {
                            newPort.initialize(nl[i]);
                        } catch (Exception e) {
                            throw new KETLThreadException(e, this);
                        }
                        // as it has not already been initialized then mark it as not used
                        newPort.used(false);
                        if (this.hmOutports.put(newPort.mstrName, newPort) != null)
                            throw new KETLThreadException("Duplicate OUT port name exists, check step "
                                    + this.getName() + " port " + newPort.mstrName, this);
                    }
                }
            }
            port = (ETLOutPort) this.hmOutports.get(arg0);
        }

        return port;
    }

    final protected Class[] getOutputRecordDatatypes(String pChannel) throws ClassNotFoundException,
            KETLThreadException {

        Class[] result = (Class[]) this.mChannelClassMapping.get(pChannel);
        if (result == null) {
            // slim the outputs down so only the used ones get used, make life easier on the garbage collector
            String[] channels = getChannels((Element) this.getXMLConfig());

            for (int x = 0; x < channels.length; x++) {
                if (channels[x].equals(pChannel)) {

                    Node[] nList = XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "CHANNEL", channels[x]);

                    ArrayList al = new ArrayList();
                    int portIndex = 0;
                    for (int i = 0; i < nList.length; i++) {
                        if (this.portUsed(pChannel, ((Element) nList[i]).getAttribute("NAME"))) {
                            ETLOutPort port = ((ETLOutPort) this.getOutPort(((Element) nList[i]).getAttribute("NAME")));
                            al.add(port.getPortClass());
                            port.setIndex(portIndex);
                            this.mHmOutportIndex.put(port, portIndex++);
                        }
                    }

                    Class[] cls = new Class[al.size()];
                    al.toArray(cls);
                    this.mChannelClassMapping.put(channels[x], cls);
                    this.setOutputRecordDataTypes(cls, pChannel);
                    return cls;
                }
            }
        }
        else
            return result;

        throw new KETLThreadException("Invalid channel request", this);

    }

    /**
     * @return
     */
    final public int getQueueSize() {
        return this.queueSize;
    }

    abstract protected String getRecordExecuteMethodFooter();

    abstract protected String getRecordExecuteMethodHeader() throws KETLThreadException;

    final protected int getRecordsProcessed() {
        return this.recordCount;
    }

    public int getUsedPortIndex(ETLPort port) throws KETLThreadException {
        if (port instanceof ETLOutPort)
            return ((Integer) this.mHmOutportIndex.get(port)).intValue();

        return ((ETLInPort) port).getSourcePortIndex();
        /*
         * for (Object o : this.mhmInportIndex.values()) { ArrayList al = (ArrayList) o; int res = al.indexOf(port); if
         * (res >= 0) return res; }
         */
        // throw new KETLThreadException("Port index not found");
    }

    final protected void getUsedPortsFromWorker(ETLWorker pWorker, String port) throws KETLThreadException {

        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Registering port usage for step "
                + pWorker.toString() + " by step " + this.toString());
        // get each in port and then call to source step(s) to request port definition
        Node[] nl = com.kni.etl.util.XMLHelper.getElementsByName(this.getXMLConfig(), "IN", "*", "*");
        registerUsedPorts(pWorker, nl, "pInputRecords");
    }

    final public Element getXMLConfig() {
        return (Element) xmlConfig;
    }

    public Object handleEventCode(int eventCode) {
        return null;
    }

    public Object handleException(Exception e) throws Exception {
        return null;
    }

    public Object handlePortEventCode(int eventCode, int portIndex) throws Exception {
        return null;
    }

    public Object handlePortException(Exception e, int portIndex) throws Exception {
        return null;
    }

    protected int hash(Object[] obj, int paths) {
        if (pos == paths)
            pos = 0;
        return pos++;
    }

    final private boolean implementsBatchManagement() {
        if (this instanceof BatchManager) {
            return true;
        }
        return false;
    }

    final public void initialize(KETLJobExecutor mkjExecutor) throws KETLThreadException {
        ((ETLStep) this).setJobExecutor(mkjExecutor);
        if (this.initialize(this.getXMLConfig()) != 0)
            throw new KETLThreadException("Core failed to initialize, see previous errors", this);
    }

    abstract protected int initialize(Node xmlConfig) throws KETLThreadException;

    final public void initializeAllOutPorts() throws KETLThreadException {

        // check for it
        Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "*", "*");
        if (nl != null) {
            for (int i = 0; i < nl.length; i++) {
                String nm = XMLHelper.getAttributeAsString(nl[i].getAttributes(), "NAME", null);
                if (nm == null || this.hmOutports.containsKey(nm) == false) {
                    ETLOutPort newPort = this.getNewOutPort((ETLStep) this);
                    try {
                        newPort.initialize(nl[i]);
                    } catch (Exception e) {
                        throw new KETLThreadException(e, this);
                    }
                    // as it has not already been initialized then mark it as not used
                    newPort.used(false);
                    if (this.hmOutports.put(newPort.mstrName, newPort) != null)
                        throw new KETLThreadException("Duplicate OUT port name exists, check step " + this.getName()
                                + " port " + newPort.mstrName, this);
                }
            }
        }
    }

    protected void initializeOutports(ETLPort[] outPortNodes) throws KETLThreadException {

        for (int i = 0; i < outPortNodes.length; i++) {
            ETLPort port = outPortNodes[i];

            if (port.isConstant()) {
                port.instantiateConstant();
            }
            else if (port.containsCode()) {
                if (port.getPortClass() == null) {
                    throw new KETLThreadException("For code based transforms DATATYPE must be specified, check step "
                            + this.getName(), this);
                }
            }
            else {
                ETLPort in = port.getAssociatedInPort();
                try {
                    if (port.useInheritedDataType() == false) {
                        if (in == null)
                            throw new KETLThreadException("Specified in port for " + this.getName() + "."
                                    + port.getPortName() + " does not exist", this);
                        port.setDataTypeFromPort(in);
                    }
                } catch (Exception e) {
                    throw new KETLThreadException(e, this);
                }
            }
        }
    }

    abstract public void initializeQueues();

    final protected void instantiateCore(Class arg0) throws KETLThreadException {
        try {

            DefaultCore newCore;
            if (this instanceof DefaultCore)
                newCore = (DefaultCore) this;
            else {
                newCore = (DefaultCore) arg0.newInstance();
                ((ETLCore) newCore).setOwner(this);
            }

            this.setCore(newCore);

            this.setBatchManager(this.getBatchManager());

        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }
    }

    final protected boolean portUsed(String pChannel, String pPort) {
        HashSet al = (HashSet) this.mChannelPortsUsed.get(pChannel);
        return al.contains(pPort);
    }

    final void postSourceConnectedInitialize() throws KETLThreadException {

        ArrayList al = new ArrayList();
        Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "*", "*");
        if (nl != null) {
            for (int i = 0; i < nl.length; i++) {
                ETLPort p = this.getOutPort(XMLHelper.getAttributeAsString(nl[i].getAttributes(), "NAME", null));

                if (p != null)
                    al.add(p);

            }

            mOutPorts = new ETLOutPort[al.size()];

            al.toArray(mOutPorts);
        }

        al.clear();
        nl = XMLHelper.getElementsByName(this.getXMLConfig(), "IN", "*", "*");
        if (nl != null) {

            for (int i = 0; i < nl.length; i++) {
                ETLPort p = this.getInPort(XMLHelper.getAttributeAsString(nl[i].getAttributes(), "NAME", null));

                if (p != null)
                    al.add(p);
            }

            mInPorts = new ETLInPort[al.size()];

            al.toArray(mInPorts);
        }
        this.initializeOutports(mOutPorts);
    }

    private HashMap mFanInWorkerUsed = new HashMap();

    final protected void registerUsedPorts(ETLWorker pWorker, Node[] nl, String objectNameInCode)
            throws KETLThreadException {
        Node wildCardPort = null;
        Node wildCardOut = null;
        ETLWorker duplicateSource = (ETLWorker) this.mFanInWorkerUsed.get(pWorker.mstrName);
        if (duplicateSource == null) {

            Node[] outNodes = XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "*", "*");
            HashSet outExists = new HashSet();
            if (outNodes != null) {
                for (int i = 0; i < outNodes.length; i++) {
                    String content = XMLHelper.getTextContent(outNodes[i]);
                    if (content != null && content.trim().equals("*")) {
                        wildCardOut = outNodes[i];
                        this.getXMLConfig().removeChild(wildCardOut);
                    }
                    else {
                        String portName = XMLHelper.getAttributeAsString(outNodes[i].getAttributes(), "NAME", null);

                        if (portName == null && content != null) {
                            content = content.trim();
                            if (content.startsWith(EngineConstants.VARIABLE_PARAMETER_START)
                                    && content.endsWith(EngineConstants.VARIABLE_PARAMETER_END)) {
                                String tmp[] = EngineConstants.getParametersFromText(content);
                                if (tmp != null && tmp.length == 1) {
                                    portName = tmp[0];
                                }
                                else
                                    portName = null;
                            }
                        }

                        if (portName != null)
                            outExists.add(portName);

                    }
                }
            }
            HashSet srcPortsUsed = new HashSet();
            for (int i = 0; i < nl.length; i++) {
                Node node = nl[i];

                ETLOutPort srcPort = null;
                ETLInPort newPort = this.getNewInPort((ETLStep) pWorker);

                // register name of variable for code generation
                newPort.setCodeGenerationReferenceObject(objectNameInCode);

                // is current port a constant, if so just instantiate it and don't do this bit
                if (ETLPort.containsConstant(XMLHelper.getTextContent(node)) == false) {
                    String[] sources = extractPortDetails(XMLHelper.getTextContent(node));

                    if (sources == null)
                        continue;

                    ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "getUsedPortsFromWorker -> Port: "
                            + ((Element) node).getAttribute("NAME") + " ");

                    if (sources[PORT].equals("*")) {
                        // wildcard port defer creation until the end
                        if (wildCardPort != null)
                            throw new KETLThreadException("Duplicate wild card IN port exists, check step "
                                    + this.getName() + " port XML -> " + XMLHelper.outputXML(node), this);
                        wildCardPort = node;
                        newPort = null;
                    }
                    else {
                        srcPort = pWorker.setOutUsed(sources[CHANNEL], sources[PORT]);
                        srcPortsUsed.add(srcPort);
                        newPort.setSourcePort(srcPort);
                        ArrayList res = (ArrayList) this.mhmInportIndex.get(objectNameInCode);
                        if (res == null) {
                            res = new ArrayList();
                            this.mhmInportIndex.put(objectNameInCode, res);
                        }

                        res.add(newPort);
                    }
                }
                else {
                    ArrayList res = (ArrayList) this.mhmInportIndex.get(objectNameInCode);
                    if (res == null) {
                        res = new ArrayList();
                        this.mhmInportIndex.put(objectNameInCode, res);
                    }

                    res.add(newPort);
                }

                if (newPort != null) {
                    try {
                        newPort.initialize(node);
                        if (srcPort != null)
                            newPort.setDataTypeFromPort(srcPort);
                    } catch (Exception e) {
                        throw new KETLThreadException(e, this);
                    }

                    if (this.hmInports.put(newPort.mstrName, newPort) != null)
                        throw new KETLThreadException("Duplicate IN port name exists, check step " + this.getName()
                                + " port " + newPort.mstrName, this);
                }

            }

            if (wildCardPort != null) {
                ArrayList otherPorts = new ArrayList();

                pWorker.initializeAllOutPorts();

                String[] sources = extractPortDetails(XMLHelper.getTextContent(wildCardPort));

                Node parent = wildCardPort.getParentNode();

                String channel = (sources.length == 3 ? sources[CHANNEL] : null);
                parent.removeChild(wildCardPort);
                NamedNodeMap nm = wildCardPort.getAttributes();

                for (Object o : pWorker.hmOutports.values()) {
                    ETLOutPort src = (ETLOutPort) o;

                    if (channel != null && src.getChannel().equals(channel) == false)
                        continue;
                    if (srcPortsUsed.contains(src))
                        continue;

                    ETLPort ePort = (ETLPort) this.hmInports.get(src.mstrName);
                    if (ePort != null && ePort.isConstant())
                        continue;

                    if (ePort != null) {
                        throw new KETLThreadException(
                                "IN port already exists from another source with the same name check step "
                                        + this.getName() + " port " + src.mstrName, this);
                    }

                    Element e = parent.getOwnerDocument().createElement("IN");

                    for (int i = 0; i < nm.getLength(); i++) {
                        Node n = nm.item(i);
                        if (n instanceof Attr)
                            e.setAttribute(((Attr) n).getName(), ((Attr) n).getValue());
                    }

                    e.setAttribute("NAME", src.mstrName);
                    e.setTextContent(pWorker.mstrName + "." + src.getChannel() + "." + src.mstrName);

                    parent.appendChild(e);
                    otherPorts.add(e);
                }

                if (otherPorts.size() > 0) {
                    nl = new Node[otherPorts.size()];
                    otherPorts.toArray(nl);
                    registerUsedPorts(pWorker, nl, objectNameInCode);
                }
            }

            if (wildCardOut != null) {
                NamedNodeMap nm = wildCardOut.getAttributes();
                for (Object o : hmInports.values()) {
                    ETLInPort export = (ETLInPort) o;

                    if (outExists.contains(export.mstrName))
                        continue;

                    Element e = this.getXMLConfig().getOwnerDocument().createElement("OUT");

                    for (int i = 0; i < nm.getLength(); i++) {
                        Node n = nm.item(i);
                        if (n instanceof Attr && ((Attr) n).getName().equals("DATATYPE") == false)
                            e.setAttribute(((Attr) n).getName(), ((Attr) n).getValue());
                    }

                    e.setAttribute("NAME", export.mstrName);
                    e.setTextContent(EngineConstants.VARIABLE_PARAMETER_START + export.mstrName
                            + EngineConstants.VARIABLE_PARAMETER_END);

                    this.getXMLConfig().appendChild(e);
                }
            }

            this.mFanInWorkerUsed.put(pWorker.mstrName, pWorker);

        }
        else {
            for (Object o : duplicateSource.hmOutports.values()) {
                ETLOutPort p = (ETLOutPort) o;
                if (p.isUsed())
                    pWorker.setOutUsed(p.getChannel(), p.mstrName);
            }
            pWorker.initializeAllOutPorts();
        }

        // resolve wildcard out.
    }

    final protected boolean isMemoryLow(long pLowMemoryThreshold) {
        Runtime r = Runtime.getRuntime();
        long free = (r.maxMemory() - (r.totalMemory() - r.freeMemory()));
        if (free < pLowMemoryThreshold)
            return true;

        return false;
    }

    private boolean controlledExit = false;

    final public void run() {
        try {
            try {
                synchronized (this.mThreadManager) {
                    ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Alive");
                }
                this.executeWorker();
                this.complete();
                controlledExit = true;
            } catch (java.lang.Error e) {
                throw new KETLThreadException(e, this);
            }
        } catch (InterruptedException e) {
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Worker interrupted");
            controlledExit = true;
            this.interruptAllSteps();
        } catch (Throwable e) {
            controlledExit = true;
            if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Worker interrupted");
                this.interruptAllSteps();
            }
            else if (this instanceof ETLStep) {
                ETLStep step = (ETLStep) this;
                step.getJobExecutor().getCurrentETLJob().getStatus().setException(e);
                step.getJobExecutor().getCurrentETLJob().getStatus().setErrorMessage(e.getMessage());
            }
            this.interruptAllSteps();
        } finally {
            if (controlledExit == false) {
                ResourcePool
                        .LogMessage(this, ResourcePool.ERROR_MESSAGE,
                                "Step has shutdown in a non controlled, cause is unknown and all other steps will be interrupted");
                this.interruptAllSteps();
            }
        }
    }

    private boolean mFailAll = false;

    abstract protected void interruptExecution() throws InterruptedException;

    final private void interruptAllSteps() {
        this.mThreadManager.jobThreadGroup.interrupt();
        mFailAll = true;
    }

    final public boolean cleanShutdown() {
        return this.controlledExit;
    }

    final public boolean failAll() {
        return this.mFailAll;
    }

    final public void selfTune(boolean arg0) {
        selfTune = arg0;
    }

    abstract protected void setBatchManager(BatchManager batchManager);

    abstract void setCore(DefaultCore newCore);

    abstract void setOutputRecordDataTypes(Class[] pClassArray, String pChannel);

    final public ETLOutPort setOutUsed(String pChannel, String pPort) throws KETLThreadException {

        // get port
        if (this.getOutPort(pPort) == null)
            throw new KETLThreadException("Invalid port name " + this.mstrName + "." + pPort, this);

        if (pChannel == null) {
            pChannel = ETLWorker.getChannels((Element) this.getXMLConfig())[DEFAULT];
        }

        ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "setOutUsed -> Source Step: " + this.toString()
                + "\tChannel: " + pChannel + "\tPort: " + pPort);

        ETLOutPort port = (ETLOutPort) this.hmOutports.get(pPort);

        HashSet al = (HashSet) this.mChannelPortsUsed.get(pChannel);

        if (al == null) {
            al = new HashSet();
            this.mChannelPortsUsed.put(pChannel, al);
        }

        al.add(pPort);

        port.used(true);

        if (port.getPortClass() == null) {
            try {
                port.setDataTypeFromPort(port.getAssociatedInPort());
            } catch (ClassNotFoundException e) {
                throw new KETLThreadException(e, this);
            }
        }

        return port;
    }

    @Override
    public String toString() {
        return this.mstrName + "(" + this.partitionID + ")";

    }

    private BatchOptimizer mBatchOptimizer = null;

    final protected void updateThreadStats(int rowCount) {
        recordCount += rowCount;

        if (mBatchOptimizer != null && selfTune && recordCount > tuneInterval) {
            this.mBatchOptimizer.optimize(this);
        }
    }

    protected abstract void close(boolean success);

    public void closeStep(boolean success) {
        this.close(success);
    }

    abstract public boolean success();

    private NumberFormat nFormat = NumberFormat.getNumberInstance();
    private static final double nano = Math.pow(10, 9);
    private static final long nanoToMilli = (long) Math.pow(10, 6);

    public String getTiming() {
        if (timing == false)
            return "N/A";

        return "" + nFormat.format(this.totalTimeNano / nano) + " seconds";
    }

    public long getCPUTiming() {
        return this.totalTimeNano / nanoToMilli;
    }

    private Object mWaitingFor = null;

    public void setWaiting(Object arg0) {
        if (arg0 == null)
            this.startTimeNano = System.nanoTime();

        this.mWaitingFor = arg0;
    }

    public boolean isWaiting() {
        return mWaitingFor == null ? false : true;
    }

    public Object waitingFor() {
        return mWaitingFor;
    }

    abstract public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue);

}
