/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

/**
 * @author nwakefield Creation Date: Jul 3, 2003
 */
public abstract class QAEventGenerator extends QA {

    final static String[] abrevs = { null, "k", "mb", "gb", "tb" };
    static final String[] timeAbrevs = { null, "d", "m", "s", "h" };
    static final int[] timeSizes = { 1, 86400, 60, 1, 60 * 60 };
    static final BigDecimal[] sizes = { new BigDecimal("1"), new BigDecimal("1024"), new BigDecimal("1048576"),
            new BigDecimal("1073741824"), new BigDecimal("1099511627776") };

    public static final String AMOUNT_TAG = "AMOUNT";
    static int BYTE_POS = 0;
    public final static String DATE_ATTRIB = "DATE";
    public final static String ERROR_ATTRIB = "ERROR";
    public final static String CHECKEVERY_ATTRIB = "CHECKEVERY";
    public final static String CHECKFIRST_ATTRIB = "CHECKFIRST";
    public static final String ERROR_COUNT_ATTRIB = "ERROR_COUNT";
    public static final String AVG_VALUE_TAG = "AVERAGE_VALUE";
    public final static String EVENT_ATTRIB = "EVENT";
    public final static String COUNT_ATTRIB = "COUNT";
    public static final String FILE_TAG = "FILE";
    public static final String QUERY_TAG = "QUERY";
    public static final String AGE_TAG = "AGE";
    public static final String ERROR_CODE_ATTRIB = "ERRORCODE";
    public static final String LOG_ON_WARNING_ATTRIB = "LOGONWARNING";
    static int GIGA_POS = 3;
    public final static int INITIALIZE = 0;
    public final static int ITEM = 2;
    public static final String ITEMCHECK_TAG = "ITEMCHECK";
    public final static String JOB_ATTRIB = "JOB_ID";
    static int KIL_POS = 1;
    public static String MAXERRORS_ATTRIB = "MAXERRORS";
    public static String MAXSIZE_ATTRIB = "MAXSIZE";
    public static String MAXVARIANCE_ATTRIB = "MAXVARIANCE";
    public static String MINVARIANCE_ATTRIB = "MINVARIANCE";
    public static String AGGREGATE_ATTRIB = "AGGREGATE";
    public static String BASEVALUE_ATTRIB = "BASEVALUE";
    public static String INDIVIDUAL_ATTRIB = "INDIVIDUAL";
    public static String COMPARE_TAG = "COMPARE";
    int miErrorCode = 1;
    static int MEGA_POS = 2;
    public static String MINSIZE_ATTRIB = "MINSIZE";
    public static final String NAME_ATTRIB = "NAME";
    public static String NUMBER_ATTRIB = "NUMBER";
    public final static String QA_ATTRIB = "QA_ID";
    public final static String QA_HISTORY_TAG = "QA_HISTORY";
    public final static char quote = '"';
    public final static int RECORD = 1;
    public static final String RECORDCHECK_TAG = "RECORDCHECK";
    public static final String TYPE_ATTRIB = "TYPE";
    public static String SAMPLE_TYPE_AVERAGE = "AVERAGE";
    public static String SAMPLE_TYPE_MOVING_AVERAGE = "MOVINGAVERAGE";
    public static String SAMPLE_TYPE_DISTRIBUTION = "DISTRIBUTION";
    static final int SAMPLE_TYPE_AVERAGE_ID = 0;
    static final int SAMPLE_TYPE_MOVING_AVERAGE_ID = 1;
    static final int SAMPLE_TYPE_DISTRIBUTION_ID = 2;
    public static String SAMPLEOFFSET_ATTRIB = "SAMPLEOFFSET";
    public static String SAMPLESIZE_ATTRIB = "SAMPLESIZE";
    public static String SAMPLETYPE_ATTRIB = "SAMPLETYPE";
    public static final String SIZE_ATTRIB = "SIZE";
    public static final String SIZE_TAG = "SIZE";
    public static final String AGGREGATE_SIZE_TAG = "AGGREGATESIZE";
    public final static String STEP_ATTRIB = "STEP_NAME";
    public final static String STRUCTURE_TAG = "STRUCTURE";
    public final static String VALUE_TAG = "VALUE";
    public final static String VALUES_TAG = "VALUES";
    public final static String VALID_TAG = "VALID";
    public final static String INVALID_TAG = "INVALID";
    static int TERA_POS = 4;
    public final static String TYPEID_ATTRIB = "TYPE_ID";

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
                        Integer val = Integer.parseInt(size.substring(0, size.length()
                                - QAEventGenerator.timeAbrevs[i].length()));

                        return val * QAEventGenerator.timeSizes[i];
                    } catch (NumberFormatException e1) {
                        return null;
                    }
                }
            }
        }
        return null;
    }

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

    public String getQAType() {
        return this.nQADefinition.getNodeName();
    }

    static BigDecimal convertToPercentageFromString(String str) {
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
            ResourcePool.LogMessage(Thread.currentThread().getName(), ResourcePool.ERROR_MESSAGE,
                    "Number format invalid for percentage conversion: " + str);

            return null;
        }
    }

    final static String[] getHistory(String job_id, String step_name, String qa_id, String qa_type, int sampleOffSet,
            int sampleSize) {
        // get history
        String[] hist = null;

        Metadata md = ResourcePool.getMetadata();

        if (md != null) {
            try {
                hist = md.getQAHistory(job_id, step_name, qa_id, qa_type, sampleOffSet, sampleSize);
            } catch (SQLException e) {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                        "Getting QA history from database - " + e.getMessage());
                System.exit(-5);
            } catch (Exception e) {
                ResourcePool.LogException(e, null);
                System.exit(-10);
            }
        }

        return hist;
    }

    String mstrHistoryXML = null;
    String mstrQAName = null;
    boolean mbLogOnWarning = false;

    protected final void fireEvent(ETLEvent event) throws KETLQAException {
        List aetTriggers = null;

        if ((event != null) && (this.step != null) && ((aetTriggers = this.step.getTriggers()) != null)) {

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

    abstract void getParameters();

    public abstract String getXMLHistory();

    protected String setQAName() throws KETLThreadException {

        this.mstrQAName = XMLHelper.getAttributeAsString(this.nQADefinition.getParentNode().getAttributes(), "NAME",
                null);

        // if a name has been given for the individual test then append it the name
        String extName = XMLHelper.getAttributeAsString(this.nQADefinition.getAttributes(), "NAME", null);

        if (extName != null) {
            this.mstrQAName = this.mstrQAName + "-" + extName;
        }

        return this.mstrQAName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#initialize(com.kni.etl.ketl.ETLStep, org.w3c.dom.Node)
     */
    @Override
    public void initialize(ETLStep eStep, Node nXMLConfig) throws KETLThreadException {
        super.initialize(eStep, nXMLConfig);

        this.mstrQAName = XMLHelper.getAttributeAsString(this.nQADefinition.getParentNode().getAttributes(), "NAME",
                null);
        this.setQAName();

        ResourcePool.LogMessage(eStep, ResourcePool.INFO_MESSAGE, "Initializing QA test " + this.toString());

        this.miErrorCode = XMLHelper.getAttributeAsInt(this.nQADefinition.getAttributes(),
                QAEventGenerator.ERROR_CODE_ATTRIB, 1);

        this.mbLogOnWarning = XMLHelper.getAttributeAsBoolean(this.nQADefinition.getAttributes(),
                QAEventGenerator.LOG_ON_WARNING_ATTRIB, false);

        this.mEventToGenerate = XMLHelper.getAttributeAsString(this.nQADefinition.getAttributes(),
                QAEventGenerator.EVENT_ATTRIB, null);

        // get comparison values if offered
        NodeList nl = this.nQADefinition.getChildNodes();
        this.mComparisonValues = new ArrayList();

        if ((nl != null) && (nl.getLength() > 0)) {
            for (int i = 0; i < nl.getLength(); i++) {
                // Check to make sure that the element has the correct tag name...
                if (QAEventGenerator.COMPARE_TAG.equals(nl.item(i).getNodeName()) == true) {
                    Node n = nl.item(i);
                    String strJobID = XMLHelper.getAttributeAsString(n.getAttributes(), QAEventGenerator.JOB_ATTRIB,
                            null);
                    String strStepID = XMLHelper.getAttributeAsString(n.getAttributes(), QAEventGenerator.STEP_ATTRIB,
                            null);
                    String strQAID = XMLHelper
                            .getAttributeAsString(n.getAttributes(), QAEventGenerator.QA_ATTRIB, null);
                    String strQAType = XMLHelper.getAttributeAsString(n.getAttributes(),
                            QAEventGenerator.TYPEID_ATTRIB, null);
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

                                        String strVal = XMLHelper.getAttributeAsString(nr.getAttributes(),
                                                strAttribute, null);

                                        if (strVal != null) {
                                            val = val.add(new BigDecimal(strVal));
                                        }
                                        else {
                                            ResourcePool.LogMessage("Warning: Comparison may be invalid "
                                                    + strAttribute + " tag not found");
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

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + this.mstrQAName + "]";
    }

    public final void setXMLHistory(String strXML) {
        NamedNodeMap nm = this.nQADefinition.getAttributes();

        String strParams = " ";

        for (int i = 0; i < nm.getLength(); i++) {
            Node n = nm.item(i);

            if (n.getNodeType() == Node.ATTRIBUTE_NODE) {
                strParams = strParams + n.getNodeName() + "=\"" + n.getNodeValue() + "\" ";
            }
        }

        this.mstrHistoryXML = "<" + QAEventGenerator.QA_HISTORY_TAG + " " + QAEventGenerator.JOB_ATTRIB + "="
                + QAEventGenerator.quote + this.step.getJobExecutor().getCurrentETLJob().getJobID()
                + QAEventGenerator.quote + " " + QAEventGenerator.QA_ATTRIB + "=" + QAEventGenerator.quote
                + this.mstrQAName + QAEventGenerator.quote + " " + QAEventGenerator.STEP_ATTRIB + "="
                + QAEventGenerator.quote + this.step.toString() + QAEventGenerator.quote + " "
                + QAEventGenerator.TYPEID_ATTRIB + "=" + QAEventGenerator.quote + this.nQADefinition.getNodeName()
                + QAEventGenerator.quote + " " + QAEventGenerator.DATE_ATTRIB + "=" + QAEventGenerator.quote
                + this.step.getJobExecutor().getCurrentETLJob().getCreationDate() + QAEventGenerator.quote + strParams
                + " >" + strXML + "</" + QAEventGenerator.QA_HISTORY_TAG + ">";
    }

    /**
     * @return Returns the miErrorCode.
     */
    public int getErrorCode() {
        return this.miErrorCode;
    }

    String mEventToGenerate;
    boolean mbEventFired = false;
    ArrayList mComparisonValues;

    public boolean recordHistory() {
        if ((this.mbLogOnWarning == false) && this.mEventToGenerate.equals("WARNING") && this.mbEventFired) {
            return false;
        }

        return true;
    }
}
