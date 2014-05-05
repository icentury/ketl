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
/*
 * Created on Jul 3, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.qa;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLEvent;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.ETLTrigger;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class QAEventGenerator.
 * 
 * @author nwakefield Creation Date: Jul 3, 2003
 */
public abstract class QAEventGenerator extends QA {

	/** The Constant abrevs. */
	final static String[] abrevs = { null, "k", "mb", "gb", "tb" };

	/** The Constant timeAbrevs. */
	static final String[] timeAbrevs = { null, "d", "m", "s", "h" };

	/** The Constant timeSizes. */
	static final int[] timeSizes = { 1, 86400, 60, 1, 60 * 60 };

	/** The Constant sizes. */
	static final BigDecimal[] sizes = { new BigDecimal("1"), new BigDecimal("1024"), new BigDecimal("1048576"), new BigDecimal("1073741824"), new BigDecimal("1099511627776") };

	/** The Constant AMOUNT_TAG. */
	public static final String AMOUNT_TAG = "AMOUNT";

	/** The BYT e_ POS. */
	static int BYTE_POS = 0;

	/** The Constant DATE_ATTRIB. */
	public final static String DATE_ATTRIB = "DATE";

	/** The Constant ERROR_ATTRIB. */
	public final static String ERROR_ATTRIB = "ERROR";

	/** The Constant CHECKEVERY_ATTRIB. */
	public final static String CHECKEVERY_ATTRIB = "CHECKEVERY";

	/** The Constant CHECKFIRST_ATTRIB. */
	public final static String CHECKFIRST_ATTRIB = "CHECKFIRST";

	/** The Constant ERROR_COUNT_ATTRIB. */
	public static final String ERROR_COUNT_ATTRIB = "ERROR_COUNT";

	/** The Constant AVG_VALUE_TAG. */
	public static final String AVG_VALUE_TAG = "AVERAGE_VALUE";

	/** The Constant EVENT_ATTRIB. */
	public final static String EVENT_ATTRIB = "EVENT";

	/** The Constant COUNT_ATTRIB. */
	public final static String COUNT_ATTRIB = "COUNT";

	/** The Constant FILE_TAG. */
	public static final String FILE_TAG = "FILE";

	/** The Constant QUERY_TAG. */
	public static final String QUERY_TAG = "QUERY";

	/** The Constant AGE_TAG. */
	public static final String AGE_TAG = "AGE";

	/** The Constant ERROR_CODE_ATTRIB. */
	public static final String ERROR_CODE_ATTRIB = "ERRORCODE";

	/** The Constant LOG_ON_WARNING_ATTRIB. */
	public static final String LOG_ON_WARNING_ATTRIB = "LOGONWARNING";

	/** The GIG a_ POS. */
	static int GIGA_POS = 3;

	/** The Constant INITIALIZE. */
	public final static int INITIALIZE = 0;

	/** The Constant ITEM. */
	public final static int ITEM = 2;

	/** The Constant ITEMCHECK_TAG. */
	public static final String ITEMCHECK_TAG = "ITEMCHECK";

	/** The Constant JOB_ATTRIB. */
	public final static String JOB_ATTRIB = "JOB_ID";

	/** The KI l_ POS. */
	static int KIL_POS = 1;

	/** The MAXERROR s_ ATTRIB. */
	public static String MAXERRORS_ATTRIB = "MAXERRORS";

	/** The MAXSIZ e_ ATTRIB. */
	public static String MAXSIZE_ATTRIB = "MAXSIZE";

	/** The MAXVARIANC e_ ATTRIB. */
	public static String MAXVARIANCE_ATTRIB = "MAXVARIANCE";

	/** The MINVARIANC e_ ATTRIB. */
	public static String MINVARIANCE_ATTRIB = "MINVARIANCE";

	/** The AGGREGAT e_ ATTRIB. */
	public static String AGGREGATE_ATTRIB = "AGGREGATE";

	/** The BASEVALU e_ ATTRIB. */
	public static String BASEVALUE_ATTRIB = "BASEVALUE";

	/** The INDIVIDUA l_ ATTRIB. */
	public static String INDIVIDUAL_ATTRIB = "INDIVIDUAL";

	/** The COMPAR e_ TAG. */
	public static String COMPARE_TAG = "COMPARE";

	/** The mi error code. */
	int miErrorCode = 1;

	/** The MEG a_ POS. */
	static int MEGA_POS = 2;

	/** The MINSIZ e_ ATTRIB. */
	public static String MINSIZE_ATTRIB = "MINSIZE";

	/** The Constant NAME_ATTRIB. */
	public static final String NAME_ATTRIB = "NAME";

	/** The NUMBE r_ ATTRIB. */
	public static String NUMBER_ATTRIB = "NUMBER";

	/** The Constant QA_ATTRIB. */
	public final static String QA_ATTRIB = "QA_ID";

	/** The Constant QA_HISTORY_TAG. */
	public final static String QA_HISTORY_TAG = "QA_HISTORY";

	/** The Constant quote. */
	public final static char quote = '"';

	/** The Constant RECORD. */
	public final static int RECORD = 1;

	/** The Constant RECORDCHECK_TAG. */
	public static final String RECORDCHECK_TAG = "RECORDCHECK";

	/** The Constant TYPE_ATTRIB. */
	public static final String TYPE_ATTRIB = "TYPE";

	/** The SAMPL e_ TYP e_ AVERAGE. */
	public static String SAMPLE_TYPE_AVERAGE = "AVERAGE";

	/** The SAMPL e_ TYP e_ MOVIN g_ AVERAGE. */
	public static String SAMPLE_TYPE_MOVING_AVERAGE = "MOVINGAVERAGE";

	/** The SAMPL e_ TYP e_ DISTRIBUTION. */
	public static String SAMPLE_TYPE_DISTRIBUTION = "DISTRIBUTION";

	/** The Constant SAMPLE_TYPE_AVERAGE_ID. */
	protected static final int SAMPLE_TYPE_AVERAGE_ID = 0;

	/** The Constant SAMPLE_TYPE_MOVING_AVERAGE_ID. */
	protected static final int SAMPLE_TYPE_MOVING_AVERAGE_ID = 1;

	/** The Constant SAMPLE_TYPE_DISTRIBUTION_ID. */
	protected static final int SAMPLE_TYPE_DISTRIBUTION_ID = 2;

	/** The SAMPLEOFFSE t_ ATTRIB. */
	public static String SAMPLEOFFSET_ATTRIB = "SAMPLEOFFSET";

	/** The SAMPLESIZ e_ ATTRIB. */
	public static String SAMPLESIZE_ATTRIB = "SAMPLESIZE";

	/** The SAMPLETYP e_ ATTRIB. */
	public static String SAMPLETYPE_ATTRIB = "SAMPLETYPE";

	/** The Constant SIZE_ATTRIB. */
	public static final String SIZE_ATTRIB = "SIZE";

	/** The Constant SIZE_TAG. */
	public static final String SIZE_TAG = "SIZE";

	/** The Constant AGGREGATE_SIZE_TAG. */
	public static final String AGGREGATE_SIZE_TAG = "AGGREGATESIZE";

	/** The Constant STEP_ATTRIB. */
	public final static String STEP_ATTRIB = "STEP_NAME";

	/** The Constant STRUCTURE_TAG. */
	public final static String STRUCTURE_TAG = "STRUCTURE";

	/** The Constant VALUE_TAG. */
	public final static String VALUE_TAG = "VALUE";

	/** The Constant VALUES_TAG. */
	public final static String VALUES_TAG = "VALUES";

	/** The Constant VALID_TAG. */
	public final static String VALID_TAG = "VALID";

	/** The Constant INVALID_TAG. */
	public final static String INVALID_TAG = "INVALID";

	/** The TER a_ POS. */
	static int TERA_POS = 4;

	/** The Constant TYPEID_ATTRIB. */
	public final static String TYPEID_ATTRIB = "TYPE_ID";

	/**
	 * Convert to bytes.
	 * 
	 * @param size
	 *            the size
	 * 
	 * @return the big decimal
	 */
	public static BigDecimal convertToBytes(String size) {
		if (size == null) {
			return null;
		}

		try {
			return new BigDecimal(size);
		} catch (NumberFormatException e) {
			// couldn't convert directly therefore check for abbreviations
			for (int i = 1; i < QAEventGenerator.abrevs.length; i++) {
				String tmp = size.substring(size.length() - QAEventGenerator.abrevs[i].length());

				if (tmp.equalsIgnoreCase(QAEventGenerator.abrevs[i])) {
					try {
						tmp = size.substring(0, size.length() - QAEventGenerator.abrevs[i].length());

						return new BigDecimal(tmp).multiply(QAEventGenerator.sizes[i]);
					} catch (NumberFormatException e1) {
						return null;
					}
				}
			}
		}

		return null;
	}

	/**
	 * Convert to seconds.
	 * 
	 * @param size
	 *            the size
	 * 
	 * @return the integer
	 */
	public static Integer convertToSeconds(String size) {
		if (size == null) {
			return null;
		}

		try {
			// if pure number then its days, convert to seconds
			return Integer.parseInt(size) * 86400;

		} catch (NumberFormatException e) {
			// couldn't convert directly therefore check for abbreviations
			for (int i = 1; i < QAEventGenerator.timeAbrevs.length; i++) {
				String tmp = size.substring(size.length() - QAEventGenerator.timeAbrevs[i].length());

				if (tmp.equalsIgnoreCase(QAEventGenerator.timeAbrevs[i])) {
					try {
						Integer val = Integer.parseInt(size.substring(0, size.length() - QAEventGenerator.timeAbrevs[i].length()));

						return val * QAEventGenerator.timeSizes[i];
					} catch (NumberFormatException e1) {
						return null;
					}
				}
			}
		}
		return null;
	}

	/**
	 * Time resolution.
	 * 
	 * @param size
	 *            the size
	 * 
	 * @return the char
	 */
	public static char timeResolution(String size) {
		if (size == null) {
			return 'd';
		}

		// couldn't convert directly therefore check for abbreviations
		for (int i = 1; i < QAEventGenerator.timeAbrevs.length; i++) {
			String tmp = size.substring(size.length() - QAEventGenerator.timeAbrevs[i].length());

			if (tmp.equalsIgnoreCase(QAEventGenerator.timeAbrevs[i])) {
				return QAEventGenerator.timeAbrevs[i].charAt(0);
			}
		}

		return 'd';
	}

	/**
	 * Gets the QA type.
	 * 
	 * @return the QA type
	 */
	public String getQAType() {
		return this.getQADefinition().getNodeName();
	}

	/**
	 * Convert to percentage from string.
	 * 
	 * @param str
	 *            the str
	 * 
	 * @return the big decimal
	 */
	protected static BigDecimal convertToPercentageFromString(String str) {
		if (str == null) {
			return null;
		}

		try {
			if (str.charAt(str.length() - 1) == '%') {
				BigDecimal val = new BigDecimal(str.substring(0, str.length() - 1)).setScale(4);
				BigDecimal div = new BigDecimal("100").setScale(4);

				return val.divide(div, BigDecimal.ROUND_UNNECESSARY);
			}

			return new BigDecimal(str);
		} catch (NumberFormatException e) {
			ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE, "Number format invalid for percentage conversion: " + str);

			return null;
		}
	}

	/**
	 * Gets the history.
	 * 
	 * @param job_id
	 *            the job_id
	 * @param step_name
	 *            the step_name
	 * @param qa_id
	 *            the qa_id
	 * @param qa_type
	 *            the qa_type
	 * @param sampleOffSet
	 *            the sample off set
	 * @param sampleSize
	 *            the sample size
	 * 
	 * @return the history
	 */
	protected final static String[] getHistory(String job_id, String step_name, String qa_id, String qa_type, int sampleOffSet, int sampleSize) {
		// get history
		String[] hist = null;

		Metadata md = ResourcePool.getMetadata();

		if (md != null) {
			try {
				hist = md.getQAHistory(job_id, step_name, qa_id, qa_type, sampleOffSet, sampleSize);
			} catch (SQLException e) {
				ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Getting QA history from database - " + e.getMessage());
				System.exit(-5);
			} catch (Exception e) {
				ResourcePool.LogException(e, null);
				System.exit(-10);
			}
		}

		return hist;
	}

	/** The mstr history XML. */
	private String mstrHistoryXML = null;

	/** The mstr QA name. */
	protected String mstrQAName = null;

	/** The mb log on warning. */
	boolean mbLogOnWarning = false;

	/**
	 * Fire event.
	 * 
	 * @param event
	 *            the event
	 * 
	 * @throws KETLQAException
	 *             the KETLQA exception
	 */
	protected final void fireEvent(ETLEvent event) throws KETLQAException {
		List aetTriggers = null;

		if ((event != null) && (this.getStep() != null) && ((aetTriggers = this.getStep().getTriggers()) != null)) {

			event.setReturnCode(this.miErrorCode);

			for (Object o : aetTriggers) {
				ETLTrigger trigger = (ETLTrigger) o;
				if (trigger.getEvent().equalsIgnoreCase(event.getEventName())) {
					this.mbEventFired = true;
					trigger.execute(event);
				}
			}
		}
	}

	/**
	 * Gets the parameters.
	 * 
	 * @return the parameters
	 */
	abstract protected void getParameters();

	/**
	 * Gets the XML history.
	 * 
	 * @return the XML history
	 */
	public abstract String getXMLHistory();

	/**
	 * Sets the QA name.
	 * 
	 * @return the string
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	protected String setQAName() throws KETLThreadException {

		this.mstrQAName = XMLHelper.getAttributeAsString(this.getQADefinition().getParentNode().getAttributes(), "NAME", null);

		// if a name has been given for the individual test then append it the
		// name
		String extName = XMLHelper.getAttributeAsString(this.getQADefinition().getAttributes(), "NAME", null);

		if (extName != null) {
			this.mstrQAName = this.mstrQAName + "-" + extName;
		}

		return this.mstrQAName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.qa.QA#initialize(com.kni.etl.ketl.ETLStep,
	 * org.w3c.dom.Node)
	 */
	@Override
	public void initialize(ETLStep eStep, Node nXMLConfig) throws KETLThreadException {
		super.initialize(eStep, nXMLConfig);

		this.setQAName(XMLHelper.getAttributeAsString(this.getQADefinition().getParentNode().getAttributes(), "NAME", null));
		this.setQAName();

		ResourcePool.LogMessage(eStep, ResourcePool.INFO_MESSAGE, "Initializing QA test " + this.toString());

		this.miErrorCode = XMLHelper.getAttributeAsInt(this.getQADefinition().getAttributes(), QAEventGenerator.ERROR_CODE_ATTRIB, 1);

		this.mbLogOnWarning = XMLHelper.getAttributeAsBoolean(this.getQADefinition().getAttributes(), QAEventGenerator.LOG_ON_WARNING_ATTRIB, false);

		this.setEventToGenerate(XMLHelper.getAttributeAsString(this.getQADefinition().getAttributes(), QAEventGenerator.EVENT_ATTRIB, null));

		// get comparison values if offered
		NodeList nl = this.getQADefinition().getChildNodes();
		this.mComparisonValues = new ArrayList();

		if ((nl != null) && (nl.getLength() > 0)) {
			for (int i = 0; i < nl.getLength(); i++) {
				// Check to make sure that the element has the correct tag
				// name...
				if (QAEventGenerator.COMPARE_TAG.equals(nl.item(i).getNodeName()) == true) {
					Node n = nl.item(i);
					String strJobID = XMLHelper.getAttributeAsString(n.getAttributes(), QAEventGenerator.JOB_ATTRIB, null);
					String strStepID = XMLHelper.getAttributeAsString(n.getAttributes(), QAEventGenerator.STEP_ATTRIB, null);
					String strQAID = XMLHelper.getAttributeAsString(n.getAttributes(), QAEventGenerator.QA_ATTRIB, null);
					String strQAType = XMLHelper.getAttributeAsString(n.getAttributes(), QAEventGenerator.TYPEID_ATTRIB, null);
					String strAttribute = XMLHelper.getAttributeAsString(n.getAttributes(), "ATTRIBUTE", null);
					String[] previous = QAEventGenerator.getHistory(strJobID, strStepID, strQAID, strQAType, 0, 1);

					if (previous != null) {
						DocumentBuilderFactory dmfFactory;
						DocumentBuilder builder = null;
						Document xmlDOM = null;

						dmfFactory = DocumentBuilderFactory.newInstance();

						try {
							builder = dmfFactory.newDocumentBuilder();

							for (String element : previous) {
								xmlDOM = builder.parse(new InputSource(new StringReader(element)));

								Node root = xmlDOM.getFirstChild();

								NodeList nlx = null;

								if (root != null) {
									nlx = root.getChildNodes();
								}

								BigDecimal val = new BigDecimal(0);

								if ((nlx != null) && (nlx.getLength() > 0)) {
									for (int y = 0; y < nlx.getLength(); y++) {
										Node nr = nlx.item(y);

										String strVal = XMLHelper.getAttributeAsString(nr.getAttributes(), strAttribute, null);

										if (strVal != null) {
											val = val.add(new BigDecimal(strVal));
										} else {
											ResourcePool.LogMessage("Warning: Comparison may be invalid " + strAttribute + " tag not found");
										}
									}
								}

								if (val != null) {
									this.mComparisonValues.add(val);
								}
							}
						} catch (Exception e) {
							ResourcePool.LogException(e, this);
						}
					}
				}
			}
		}

		this.getParameters();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.getClass().getName() + "[" + this.getQAName() + "]";
	}

	/**
	 * Sets the XML history.
	 * 
	 * @param strXML
	 *            the new XML history
	 */
	public final void setXMLHistory(String strXML) {
		NamedNodeMap nm = this.getQADefinition().getAttributes();

		String strParams = " ";

		for (int i = 0; i < nm.getLength(); i++) {
			Node n = nm.item(i);

			if (n.getNodeType() == Node.ATTRIBUTE_NODE) {
				strParams = strParams + n.getNodeName() + "=\"" + n.getNodeValue() + "\" ";
			}
		}

		this.setHistoryXML("<" + QAEventGenerator.QA_HISTORY_TAG + " " + QAEventGenerator.JOB_ATTRIB + "=" + QAEventGenerator.quote
				+ this.getStep().getJobExecutor().getCurrentETLJob().getJobID() + QAEventGenerator.quote + " " + QAEventGenerator.QA_ATTRIB + "=" + QAEventGenerator.quote
				+ this.getQAName() + QAEventGenerator.quote + " " + QAEventGenerator.STEP_ATTRIB + "=" + QAEventGenerator.quote + this.getStep().toString()
				+ QAEventGenerator.quote + " " + QAEventGenerator.TYPEID_ATTRIB + "=" + QAEventGenerator.quote + this.getQADefinition().getNodeName() + QAEventGenerator.quote
				+ " " + QAEventGenerator.DATE_ATTRIB + "=" + QAEventGenerator.quote + this.getStep().getJobExecutor().getCurrentETLJob().getCreationDate() + QAEventGenerator.quote
				+ strParams + " >" + strXML + "</" + QAEventGenerator.QA_HISTORY_TAG + ">");
	}

	/**
	 * Gets the error code.
	 * 
	 * @return Returns the miErrorCode.
	 */
	public int getErrorCode() {
		return this.miErrorCode;
	}

	/** The event to generate. */
	private String mEventToGenerate;

	/** The mb event fired. */
	boolean mbEventFired = false;

	/** The comparison values. */
	ArrayList mComparisonValues;

	/**
	 * Record history.
	 * 
	 * @return true, if successful
	 */
	public boolean recordHistory() {
		if ((this.mbLogOnWarning == false) && this.getEventToGenerate().equals("WARNING") && this.mbEventFired) {
			return false;
		}

		return true;
	}

	public void setQAName(String mstrQAName) {
		this.mstrQAName = mstrQAName;
	}

	public String getQAName() {
		return mstrQAName;
	}

	public void setHistoryXML(String mstrHistoryXML) {
		this.mstrHistoryXML = mstrHistoryXML;
	}

	public String getHistoryXML() {
		return mstrHistoryXML;
	}

	public void setEventToGenerate(String mEventToGenerate) {
		this.mEventToGenerate = mEventToGenerate;
	}

	public String getEventToGenerate() {
		return mEventToGenerate;
	}
}
