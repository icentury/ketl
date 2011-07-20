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
import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.ETLJob;
import com.kni.etl.EngineConstants;
import com.kni.etl.ParameterList;
import com.kni.etl.SharedCounter;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.SystemConfigCache.Parameter;
import com.kni.etl.ketl.SystemConfigCache.ParameterType;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.qa.QACollection;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLStep.
 * 
 * @author nwakefield To change the template for this generated type comment go
 *         to Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and
 *         Comments
 */
public abstract class ETLStep extends ETLWorker {

	/** The job. */
	private ETLJob mJob;

	private Exception pendingException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#interruptExecution()
	 */
	@Override
	final protected void interruptExecution() throws InterruptedException, KETLThreadException {
		if (this.mJob.isKilled()) {
			throw new InterruptedException("Job has been killed");
		} else if (this.mJob.isPaused()) {
			this.setWaiting("Pause to be released");
			while (this.mJob.isPaused()) {
				Thread.sleep(1000);
			}
			this.setWaiting(null);
			this.interruptExecution();
		} else if (this.pendingException != null)
			throw new KETLThreadException(this.pendingException, this);
	}

	final public void setPendingException(Exception e) {
		this.pendingException = e;
	}

	/** The Constant TAGS_NOT_SUPPORTING_PARAMETERS. */
	final static String[] TAGS_NOT_SUPPORTING_PARAMETERS = { "FILTER", "OUT" };

	/** The Constant BATCHSIZE_ATTRIB. */
	public static final String BATCHSIZE_ATTRIB = "BATCHSIZE";

	/** The Constant CASE_TAG. */
	public static final String CASE_TAG = "CASE";

	public static final String IMPORTS = "IMPORTS";

	/** The Constant DEFAULT_TAG. */
	public static final String DEFAULT_TAG = "DEFAULT";

	/** The Constant DRIVING_STEP_ATTRIB. */
	public static final String DRIVING_STEP_ATTRIB = "DRIVING_STEP";

	/** The Constant ERRORLIMIT_ATTRIB. */
	public static final String ERRORLIMIT_ATTRIB = "ERRORLIMIT";

	/** The Constant FATAL_ERROR_HANDLER. */
	public static final String FATAL_ERROR_HANDLER = "FATAL_ERROR";

	/** The Constant IN_TAG. */
	public static final String IN_TAG = "IN";

	/** The Constant LOG_BAD_RECORDS. */
	public static final String LOG_BAD_RECORDS = "LOGBADRECORDS";

	/** The Constant LOG_ERROR_HANDLER. */
	public static final String LOG_ERROR_HANDLER = "LOG_ERROR";

	/** The Constant LOG_MESSAGE_HANDLER. */
	public static final String LOG_MESSAGE_HANDLER = "LOG_MESSAGE";

	/** The Constant NAME_ATTRIB. */
	public static final String NAME_ATTRIB = "NAME";

	/** The Constant OUT_TAG. */
	public static final String OUT_TAG = "OUT";

	/** The Constant SHOWEXCEPTIONS. */
	public static final String SHOWEXCEPTIONS = "SHOWEXCEPTIONS";

	/** The Constant SQL_ATTRIB. */
	public static final String SQL_ATTRIB = "SQL";

	/** The Constant TRIGGER_TAG. */
	public static final String TRIGGER_TAG = "TRIGGER";

	/** The Constant XMLSOURCE_ATTRIB. */
	public static final String XMLSOURCE_ATTRIB = "XMLSOURCE";

	/** The Constant XMLSOURCENAME_ATTRIB. */
	public static final String XMLSOURCENAME_ATTRIB = "XMLSOURCENAME";

	/** The ma parameters. */
	protected List<ParameterList> maParameters;

	/** The mkj executor. */
	protected KETLJobExecutor mkjExecutor = null;

	/** The step template. */
	private final HashMap mStepTemplate = new HashMap();

	/**
	 * The Constructor.
	 * 
	 * @param pXMLConfig
	 *            TODO
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             TODO
	 */
	public ETLStep(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

		String strParameterListName = null;

		// Find the name of the parameter list to be used...
		if ((strParameterListName = XMLHelper.getAttributeAsString(pXMLConfig.getAttributes(), EngineConstants.PARAMETER_LIST, null)) == null && this.getRequiredTags(null) != null
				&& this.getRequiredTags(null).length > 0) {
			throw new KETLThreadException("Missing required parameters: " + this.getRequiredTagsMessage(null), this);
		}

		/*
		 * get parameter list values, this will parse all parameter lists and
		 * populate maParameters with lists of complete parameters.
		 */
		if (strParameterListName != null)
			if (this.getParamaterLists(strParameterListName) != 0) {
				throw new KETLThreadException("No complete parameter sets found, check that the following exist:\n" + this.getRequiredTagsMessage(null), this);
			}

	}

	/** The mo dump buffer. */
	private OutputStream moDump, moDumpBuffer;

	/** The dump writer. */
	private Writer mDumpWriter;

	private String mGroup;
	/** The dump file. */
	private String mDumpFile;

	/** The logger failed. */
	private boolean mLoggerFailed = false;

	/**
	 * Log bad record.
	 * 
	 * @param pRowNum
	 *            the row num
	 * @param pRec
	 *            the rec
	 * @param e2
	 *            the e2
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	protected void logBadRecord(int pRowNum, Object[] pRec, Exception e2) throws IOException {
		if (this.mLoggerFailed)
			return;
		try {
			if (this.moDump == null) {
				String rootPath = this.getJobExecutor().ejCurrentJob.getLoggingPath();

				this.mDumpFile = rootPath + File.separator + this.getJobID() + "." + this.getJobExecutionID();
				this.moDump = new FileOutputStream(this.mDumpFile);
				this.moDumpBuffer = new BufferedOutputStream(this.moDump);
				this.mDumpWriter = new PrintWriter(this.moDumpBuffer);

				if (this.mInPorts != null) {
					this.mDumpWriter.write("Input record format(Constants excluded)\n");
					Object inCols[] = new Object[1024];
					for (ETLInPort element : this.mInPorts) {
						if (element.isConstant() == false)
							inCols[element.getSourcePortIndex()] = element.mstrName;
					}
					for (Object element : inCols) {
						if (element != null)
							this.mDumpWriter.write(element + "|");
					}
					this.mDumpWriter.write("\n");
				}
				if (this.mOutPorts != null) {
					this.mDumpWriter.write("Output record format(Constants excluded)\n");
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
					for (Object element : outCols) {
						if (element != null)
							this.mDumpWriter.write(element + "|");
					}
					this.mDumpWriter.write("\n");
				}

			}
			this.mDumpWriter.write("Row: " + pRowNum + "|");
			for (Object element : pRec) {
				this.mDumpWriter.write(element == null ? "[NULL]|" : element.toString().replace("|", "\\|") + "|");
			}

			this.mDumpWriter.write("|" + e2.toString() == null ? "[NULL]" : e2.toString().replace("|", "\\|").replace("\n", " ") + "\n");

		} catch (IOException e) {
			this.mLoggerFailed = true;
			ResourcePool.LogMessage(e, ResourcePool.ERROR_MESSAGE, "Bad record logging failed: " + e.toString());
			ResourcePool.LogException(e, this);
		}

	}

	/**
	 * Gets the job execution ID.
	 * 
	 * @return the job execution ID
	 */
	@Override
	final public long getJobExecutionID() {
		return this.getJobExecutor().getCurrentETLJob().getJobExecutionID();
	}

	/**
	 * Gets the job execution ID.
	 * 
	 * @return the job execution ID
	 */
	@Override
	final public String getJobID() {
		return this.getJobExecutor().getCurrentETLJob().getJobID();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#closeStep(boolean)
	 */
	@Override
	public void closeStep(boolean success, boolean jobSuccess) {
		super.closeStep(success, jobSuccess);
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
	 * Gets the job executor.
	 * 
	 * @return the job executor
	 */
	public KETLJobExecutor getJobExecutor() {
		return this.getThreadManager().getJobExecutor();
	}

	/**
	 * Gets the method map from system XML.
	 * 
	 * @param pMethod
	 *            the method
	 * @param pClass
	 *            the class
	 * @param pRequiredDatatype
	 *            the required datatype
	 * @param errorMessage
	 *            the error message
	 * 
	 * @return the method map from system XML
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	protected String getMethodMapFromSystemXML(String pMethod, Class pClass, Class pRequiredDatatype, String errorMessage) throws KETLThreadException {
		throw new KETLThreadException(errorMessage, this);
	}

	/**
	 * Gets the paramater lists.
	 * 
	 * @param strParameterListName
	 *            the str parameter list name
	 * 
	 * @return the paramater lists
	 */
	protected int getParamaterLists(String strParameterListName) {
		ArrayList tmp = this.getJobExecutor().getCurrentETLJob().getParameterLists(strParameterListName);
		ArrayList res = new ArrayList();

		for (Object o : tmp) {
			if (this.hasCompleteParameterSet((com.kni.etl.ParameterList) o, null)) {
				res.add(o);
			}
		}

		if (res.size() == 0)
			return 5;

		this.maParameters = res;
		return 0;

	}

	/**
	 * Gets the parameter value.
	 * 
	 * @param iParamList
	 *            The param list
	 * @param strParamName
	 *            the str param name
	 * 
	 * @return the parameter value
	 */
	public String getParameterValue(int iParamList, String strParamName) {
		if (this.maParameters != null) {
			ParameterList paramList = this.maParameters.get(iParamList);

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

	public Map getParameterListValues(int iParamList) {
		if (this.maParameters != null) {
			ParameterList paramList = this.maParameters.get(iParamList);

			if (paramList == null) {
				return null;
			}

			return paramList.getParameters();
		}

		return null;
	}

	/**
	 * Gets the QA class.
	 * 
	 * @param strQAType
	 *            the str QA type
	 * 
	 * @return the QA class
	 */
	public String getQAClass(String strQAType) {
		// TODO Auto-generated method stub
		return null;
	}

	private final SystemConfig systemConfig = SystemConfigCache.getInstance();

	/**
	 * Gets the required tags.
	 * 
	 * @return the required tags
	 */
	protected String[] getRequiredTags(String group) {
		return systemConfig.getRequiredTags(this.getClass(), group);
	}

	protected String[] getRequiredTags() {
		return this.getRequiredTags(this.getGroup());
	}

	protected String getRequiredTagsMessage() {
		return this.getRequiredTagsMessage(this.getGroup());
	}

	/**
	 * Gets the required tags message.
	 * 
	 * @return the required tags message
	 */
	protected String getRequiredTagsMessage(String group) {
		if (this.getRequiredTags(group) == null) {
			return "Step is missing getRequiredTags(), coding error, please report bug";
		}

		String msg = "";

		for (int i = 0; i < this.getRequiredTags(group).length; i++) {
			String str = this.getRequiredTags(group)[i];
			msg = msg + "\t" + str + "\n";
		}

		return msg;
	}

	/**
	 * Gets the step template.
	 * 
	 * @param pGroup
	 *            the group
	 * @param pName
	 *            the name
	 * @param pDefaultAllowed
	 *            the default allowed
	 * 
	 * @return the step template
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	protected final String getStepTemplate(String pGroup, String pName, boolean pDefaultAllowed) throws KETLThreadException {

		return this.getStepTemplate(this.getClass(), pGroup, pName, pDefaultAllowed);
	}

	protected final String getAttribute(String pGroup, String pName, boolean pDefaultAllowed) throws KETLThreadException {

		return this.getAttribute(this.getClass(), pGroup, pName, pDefaultAllowed);
	}

	protected final String getAttribute(Class parentClass, String pGroup, String pName, boolean pDefaultAllowed) throws KETLThreadException {

		Parameter p = this.systemConfig.getParameterOfType(parentClass, pGroup, pName, ParameterType.ATTRIBUTE, pDefaultAllowed);
		if (p == null)
			return null;

		return p.defaultValue.toString();
	}

	/**
	 * Gets the step template.
	 * 
	 * @param parentClass
	 *            the parent class
	 * @param pGroup
	 *            the group
	 * @param pName
	 *            the name
	 * @param pDefaultAllowed
	 *            the default allowed
	 * 
	 * @return the step template
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	protected final String getStepTemplate(Class parentClass, String pGroup, String pName, boolean pDefaultAllowed) throws KETLThreadException {
		return this.systemConfig.getStepTemplate(parentClass, pGroup, pName, pDefaultAllowed);
	}

	/**
	 * Checks for complete parameter set.
	 * 
	 * @param aParametersAndValues
	 *            the a parameters and values
	 * 
	 * @return true, if successful
	 */
	private boolean hasCompleteParameterSet(com.kni.etl.ParameterList aParametersAndValues, String group) {
		if (this.getRequiredTags(group) == null) {
			return true;
		}

		for (int i = 0; i < this.getRequiredTags(group).length; i++) {
			boolean found = false;

			if (aParametersAndValues.getParameter(this.getRequiredTags(group)[i]) != null) {
				found = true;
			}

			if (found == false) {
				return false;
			}
		}

		return true;
	}

	/** The DEFAUL t_ BATCHSIZE. */
	protected static int DEFAULT_BATCHSIZE = 1000;

	/** The show exceptions. */
	protected boolean mbLogBadRecords = false, mShowExceptions = false;

	/** The DEFAUL t_ ERRORLIMIT. */
	protected static int DEFAULT_ERRORLIMIT = 0;

	/** The mi error limit. */
	private int miErrorLimit = 0;

	/** The mqac QA collection. */
	private QACollection mqacQACollection;

	/** The mv triggers. */
	private final List mvTriggers = new ArrayList();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#complete()
	 */
	@Override
	public int complete() throws KETLThreadException {
		this.mbLastThreadToComplete = this._isLastThreadToComplete();

		if (this.isLastThreadToEnterCompletePhase())
			this.mqacQACollection.completeCheck();

		return super.complete();
	}

	private String userDefinedImports;

	/**
	 * Record check.
	 * 
	 * @param di
	 *            the di
	 * @param e
	 *            the e
	 * 
	 * @throws KETLQAException
	 *             the KETLQA exception
	 */
	final protected void recordCheck(Object[] di, Exception e) throws KETLQAException {
		this.mqacQACollection.recordCheck(di, e);
		this.mqacQACollection.itemChecks(di, e);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#initialize(org.w3c.dom.Node)
	 */
	@Override
	protected int initialize(Node xmlConfig) throws KETLThreadException {

		// Get child nodes...
		try {
			NodeList nl = xmlConfig.getChildNodes();

			for (int i = 0; i < nl.getLength(); i++) {
				Node node = nl.item(i);

				if (node.getNodeName().compareTo(ETLStep.TRIGGER_TAG) == 0) {
					if (this.addTrigger(node) == null) {
						return -4;
					}
				} else {
					// Add other children as needed
				}
			}
		} catch (Exception e) {
			ResourcePool.LogMessage(this, e.getMessage());
			return -1;
		}

		this.mbFirstThreadToStart = this._isFirstThreadToStart();
		// initialize any qa for this step
		this.mqacQACollection = this.getQACollection(xmlConfig);

		this.systemConfig.refreshStepTemplates(this.getClass());

		this.batchSize = XMLHelper.getAttributeAsInt(xmlConfig.getParentNode().getParentNode().getAttributes(), ETLStep.BATCHSIZE_ATTRIB, ETLStep.DEFAULT_BATCHSIZE);
		this.userDefinedImports = XMLHelper.getAttributeAsString(xmlConfig.getParentNode().getParentNode().getAttributes(), ETLStep.IMPORTS, null);

		this.mbLogBadRecords = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), ETLStep.LOG_BAD_RECORDS, this.mbLogBadRecords);

		this.mShowExceptions = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), ETLStep.SHOWEXCEPTIONS, this.mShowExceptions);

		// Pull the error limit...
		this.miErrorLimit = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), ETLStep.ERRORLIMIT_ATTRIB, ETLStep.DEFAULT_ERRORLIMIT);

		/*
		 * String strParameterListName = null; // Find the name of the parameter
		 * list to be used... if ((strParameterListName =
		 * XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
		 * EngineConstants.PARAMETER_LIST, null)) == null &&
		 * this.getRequiredTags() != null && this.getRequiredTags().length > 0)
		 * { throw new KETLThreadException("Missing required parameters: " +
		 * this.getRequiredTagsMessage()); } get parameter list values, this
		 * will parse all parameter lists and populate maParameters with lists
		 * of complete parameters. if (strParameterListName != null) if
		 * (getParamaterLists(strParameterListName) != 0) {
		 * ResourcePool.LogMessage(this, "No complete parameter sets found,
		 * check that the following exist:\n" + getRequiredTagsMessage());
		 * return 4; }
		 */
		this.mErrorCounter = this.getJobExecutor().ejCurrentJob.getErrorCounter(this.getName());

		if (this.isFirstThreadToEnterInitializePhase())
			this.mqacQACollection.initializeCheck();

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreImports()
	 */
	@Override
	protected String generateCoreImports() {
		if (userDefinedImports != null && this.userDefinedImports.trim().length() > 0) {
			return super.generateCoreImports() + ("import " + userDefinedImports.replace(",", ";\nimport ") + ";\n");
		}
		return super.generateCoreImports();
	}

	/**
	 * Adds the trigger.
	 * 
	 * @param xmlNode
	 *            the xml node
	 * 
	 * @return the ETL trigger
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	protected ETLTrigger addTrigger(Node xmlNode) throws KETLThreadException {
		ETLTrigger tTrigger = new ETLTrigger(this);

		// Call the initialize() method ourselves to get any errors in the
		// config...
		if (tTrigger.initialize(xmlNode, this) != 0) {
			ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "unable to create trigger in step '" + this.getName() + "'.");

			return null;
		}

		this.mvTriggers.add(tTrigger);

		return tTrigger;
	}

	/**
	 * Gets the QA collection.
	 * 
	 * @param xmlConfig
	 *            the xml config
	 * 
	 * @return the QA collection
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	private QACollection getQACollection(Node xmlConfig) throws KETLThreadException {
		return this.getJobExecutor().getQACollection(this.getName(), this, xmlConfig);
	}

	/**
	 * Gets the QA collection.
	 * 
	 * @return the QA collection
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	protected QACollection getQACollection() throws KETLThreadException {

		if (this.mqacQACollection == null)
			this.mqacQACollection = this.getQACollection(this.getXMLConfig());
		return this.mqacQACollection;
	}

	/**
	 * Record to log.
	 * 
	 * @param entry
	 *            the entry
	 * @param info
	 *            the info
	 */
	public void recordToLog(Object entry, boolean info) {

		if (this.getJobExecutor() == null || this.getJobExecutor().ejCurrentJob == null)
			return;

		java.util.Date dt = new java.util.Date();
		ArrayList log = this.getJobExecutor().ejCurrentJob.getLog(this.getName());
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

	/**
	 * Gets the log.
	 * 
	 * @return the log
	 */
	public ArrayList getLog() {
		return this.getJobExecutor().ejCurrentJob.getLog(this.getName());
	}

	/**
	 * Sets the job executor.
	 * 
	 * @param executor
	 *            the executor
	 */
	public void setJobExecutor(KETLJobExecutor executor) {
		this.mkjExecutor = executor;
		this.mJob = this.mkjExecutor.ejCurrentJob;
	}

	/**
	 * Show exception.
	 * 
	 * @return true, if successful
	 */
	public boolean showException() {
		return this.mShowExceptions;
	}

	/** The error counter. */
	SharedCounter mErrorCounter = null;

	/**
	 * Gets the error count.
	 * 
	 * @return the error count
	 */
	public int getErrorCount() {
		return this.mErrorCounter.value();
	}

	/**
	 * Gets the last exception.
	 * 
	 * @return the last exception
	 */
	public Exception getLastException() {
		return this.mLastError;
	}

	/** The last error. */
	private Exception mLastError = null;

	/** The mb first thread to start. */
	private boolean mbFirstThreadToStart = false;

	/** The mb last thread to complete. */
	private boolean mbLastThreadToComplete = false;

	/**
	 * Increment error count.
	 * 
	 * @param e
	 *            the e
	 * @param i
	 *            the i
	 * @param recordCounter
	 *            the record counter
	 * 
	 * @throws Exception
	 *             the exception
	 */
	protected void incrementErrorCount(Exception e, int i, int recordCounter) throws Exception {

		this.mLastError = e;
		if (this.mErrorCounter.increment(i) > this.miErrorLimit) {
			if (this.miErrorLimit == 0) {
				throw e;
			}
			throw e;
		}

	}

	/**
	 * Increment error count.
	 * 
	 * @param event
	 *            the event
	 * @param i
	 *            the i
	 * 
	 * @throws KETLQAException
	 *             the KETLQA exception
	 */
	protected void incrementErrorCount(ETLEvent event, int i) throws KETLQAException {

		if (this.mErrorCounter.increment(i) > this.miErrorLimit) {
			throw new KETLQAException("Step halted, QA failed, see below for details: " + event.mstrMessage, event, this);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#success()
	 */
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

	/** The mb case check performed already. */
	private boolean mbCaseCheckPerformedAlready = false;

	/**
	 * Matches event handler.
	 * 
	 * @param strHandler
	 *            the str handler
	 * @param strRequiredHandler
	 *            the str required handler
	 * 
	 * @return true, if successful
	 */
	protected boolean matchesEventHandler(String strHandler, String strRequiredHandler) {
		if (strHandler.equals(strRequiredHandler)) {
			return true;
		}

		if ((this.mbCaseCheckPerformedAlready == false) && strHandler.equalsIgnoreCase(strRequiredHandler)) {
			ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Event handler is \"" + strHandler + "\" and event generated is \"" + strRequiredHandler
					+ "\" possible case error in XML");
			this.mbCaseCheckPerformedAlready = true;
		}

		return false;
	}

	/**
	 * Handle event.
	 * 
	 * @param strHandler
	 *            the str handler
	 * @param event
	 *            the event
	 * 
	 * @return the int
	 * 
	 * @throws KETLQAException
	 *             the KETLQA exception
	 */
	public int handleEvent(String strHandler, ETLEvent event) throws KETLQAException {
		if (this.matchesEventHandler(strHandler, ETLStep.LOG_MESSAGE_HANDLER)) {
			return this.handleLogMessage(event);
		} else if (this.matchesEventHandler(strHandler, ETLStep.FATAL_ERROR_HANDLER)) {
			return this.handleFatalError(event);
		} else if (this.matchesEventHandler(strHandler, ETLStep.LOG_ERROR_HANDLER)) {
			return this.handleErrorMessage(event);
		}

		return 0;
	}

	/**
	 * Handle log message.
	 * 
	 * @param event
	 *            the event
	 * 
	 * @return the int
	 */
	public int handleLogMessage(ETLEvent event) {
		// to DB
		ResourcePool.LogMessage(event.getETLStep(), event.getReturnCode(), ResourcePool.INFO_MESSAGE, event.mstrMessage, event.getExtendedMessage(), true);

		// to stdout
		ResourcePool.LogMessage(event.getETLStep(), event.getReturnCode(), ResourcePool.INFO_MESSAGE, event.mstrMessage, event.getExtendedMessage(), false);

		return 1;
	}

	/**
	 * Handle error message.
	 * 
	 * @param event
	 *            the event
	 * 
	 * @return the int
	 * 
	 * @throws KETLQAException
	 *             the KETLQA exception
	 */
	public int handleErrorMessage(ETLEvent event) throws KETLQAException {
		this.incrementErrorCount(event, 1);

		return 1;
	}

	/**
	 * Handle fatal error.
	 * 
	 * @param event
	 *            the event
	 * 
	 * @return the int
	 * 
	 * @throws KETLQAException
	 *             the KETLQA exception
	 */
	public int handleFatalError(ETLEvent event) throws KETLQAException {
		ResourcePool.LogMessage(event.getETLStep(), event.getReturnCode(), ResourcePool.FATAL_MESSAGE, event.mstrMessage, event.getExtendedMessage(), true);
		throw new KETLQAException("Step halted, QA failed, see below for details: " + event.mstrMessage, event, this);
	}

	/**
	 * _is first thread to start.
	 * 
	 * @return true, if successful
	 */
	private boolean _isFirstThreadToStart() {
		ETLJob kj = this.mkjExecutor.getCurrentETLJob();
		SharedCounter cnt = kj.getCounter("STARTUP" + this.getName());

		if (cnt.increment(1) == 1)
			return true;

		return false;
	}

	/**
	 * _is last thread to complete.
	 * 
	 * @return true, if successful
	 */
	private boolean _isLastThreadToComplete() {

		ETLJob kj = this.mkjExecutor.getCurrentETLJob();

		SharedCounter cnt = kj.getCounter("SHUTDOWN" + this.getName());
		if (cnt.increment(1) == this.partitions) {
			return true;
		}
		return false;
	}

	/**
	 * Checks if is first thread to enter initialize phase.
	 * 
	 * @return true, if is first thread to enter initialize phase
	 */
	public boolean isFirstThreadToEnterInitializePhase() {
		return this.mbFirstThreadToStart;
	}

	/**
	 * Checks if is last thread to enter complete phase.
	 * 
	 * @return true, if is last thread to enter complete phase
	 */
	public boolean isLastThreadToEnterCompletePhase() {
		return this.mbLastThreadToComplete;
	}

	/**
	 * Log exception.
	 * 
	 * @param exception
	 *            the exception
	 */
	public void logException(KETLThreadException exception) {
		this.mLastError = exception;

		this.recordToLog(exception, false);

		if (this.mErrorCounter == null)
			this.mErrorCounter = this.getJobExecutor().ejCurrentJob.getErrorCounter(this.getName());

		this.mErrorCounter.increment(1);
	}

	/**
	 * Gets the triggers.
	 * 
	 * @return the triggers
	 */
	public List getTriggers() {
		return this.mvTriggers;
	}

	/**
	 * Gets the target step.
	 * 
	 * @param mstrTargetStep
	 *            the mstr target step
	 * 
	 * @return the target step
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public ETLStep getTargetStep(String mstrTargetStep) throws KETLThreadException {
		return (ETLStep) this.getThreadManager().getStep(this, mstrTargetStep);

	}

	protected void setGroup(String group) {
		this.mGroup = group;
	}

	protected String getGroup() {
		return this.mGroup;
	}

}
