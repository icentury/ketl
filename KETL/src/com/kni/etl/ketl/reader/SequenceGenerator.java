/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Mar 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.reader;

import java.text.DateFormat;
import java.util.Date;

import org.w3c.dom.Node;

import com.kni.etl.DataItemHelper;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.stringtools.FastSimpleDateFormat;
import com.kni.etl.util.DateAdd;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield Creation Date: Mar 17, 2003
 */
public class SequenceGenerator extends ETLReader implements DefaultReaderCore {

    public SequenceGenerator(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    public static final String DATATYPE = "DATATYPE";
    public static final String VALUES = "VALUES";
    int mValueCounter = 0;
    int mValuesRequested;

    class SequenceOutPort extends ETLOutPort {

        Counter counter = null;

        @Override
        public boolean containsCode() throws KETLThreadException {
            return true;
        }
        
        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            int res = super.initialize(xmlConfig);
            if (res != 0)
                return res;

            counter = new Counter();

            int type = DataItemHelper.getDataTypeIDbyName(XMLHelper.getAttributeAsString(this.getXMLConfig()
                    .getAttributes(), DATATYPE, "STRING"));

            counter.type = type;

            String startValue = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "STARTVALUE", null);
            String incrementValue = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "INCREMENT",
                    null);
            switch (type) {
            case DataItemHelper.STRING:
                throw new KETLThreadException("String not supported for sequence generator", this);
            case DataItemHelper.INTEGER:
                counter.mItem = startValue == null ? 0 : Integer.parseInt(startValue);
                counter.mIncrement = incrementValue == null ? 1 : Integer.parseInt(incrementValue);
                break;
            case DataItemHelper.LONG:
                counter.mItem = startValue == null ? new Long(0) : Long.parseLong(startValue);
                counter.mIncrement = incrementValue == null ? new Long(1) : Long.parseLong(incrementValue);
                break;
            case DataItemHelper.FLOAT:
                counter.mItem = startValue == null ? new Float(0) : Float.parseFloat(startValue);
                counter.mIncrement = incrementValue == null ? new Float(1) : Float.parseFloat(incrementValue);
                break;
            case DataItemHelper.DATE:
                String fmtStr = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), "FORMATSTRING",
                        null);

                DateFormat fm = (fmtStr == null ? DateFormat.getTimeInstance(DateFormat.LONG)
                        : new FastSimpleDateFormat(fmtStr));
                try {
                    counter.mItem = startValue == null ? new Date() : fm.parse(startValue);
                    counter.mIncrement = new DateAdd(incrementValue == null ? "1dy" : incrementValue);
                } catch (Exception e) {
                    throw new KETLThreadException(e, this);
                }
                break;
            case DataItemHelper.DOUBLE:
                counter.mItem = startValue == null ? new Double(0) : Double.parseDouble(startValue);
                counter.mIncrement = incrementValue == null ? new Double(1) : Double.parseDouble(incrementValue);

                break;
            case DataItemHelper.CHAR:
                counter.mItem = startValue == null ? new Character('a') : startValue.charAt(0);
                counter.mIncrement = incrementValue == null ? new Character((char) 1) : startValue.charAt(0);

                break;
            default:
                throw new KETLThreadException(this.getPortClass().getCanonicalName()
                        + "  not supported for sequence generator", this);
            }

            return 0;
        }
        public SequenceOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    class Counter {

        int type;
        Object mItem;
        Object mIncrement;

        Object increment() throws KETLReadException {
            Object res = mItem;
            switch (type) {
            case DataItemHelper.INTEGER:
                mItem = ((Integer) mItem).intValue() + ((Integer) mIncrement).intValue();
                break;
            case DataItemHelper.LONG:
                mItem = ((Long) mItem).longValue() + ((Long) mIncrement).longValue();
                break;
            case DataItemHelper.FLOAT:
                mItem = ((Float) mItem).floatValue() + ((Float) mIncrement).floatValue();

                break;
            case DataItemHelper.DATE:
                mItem = new Date(((Date) mItem).getTime());
                mItem = ((DateAdd) mIncrement).increment((Date) mItem);
                break;
            case DataItemHelper.DOUBLE:
                mItem = ((Double) mItem).doubleValue() + ((Double) mIncrement).doubleValue();

                break;
            case DataItemHelper.CHAR:
                mItem = ((Character) mItem).charValue() + ((Character) mIncrement).charValue();

                break;
            default:
                throw new KETLReadException("Datatype not supported for sequence generator");
            }
            return res;
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.reader.ETLReader#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node pXmlConfig) throws KETLThreadException {
        int res = super.initialize(pXmlConfig);

        this.mValuesRequested = XMLHelper.getAttributeAsInt(pXmlConfig.getAttributes(), VALUES, -1);

        if (this.mValuesRequested < 0) {
            throw new KETLThreadException("Values requested is missing or a negative number", this);
        }
        return res;
    }

    public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLReadException {

        if (mValueCounter++ < this.mValuesRequested) {
            for (int i = 0; i < this.mOutPorts.length; i++) {
                if (this.mOutPorts[i].isUsed()) {

                    if (this.mOutPorts[i].isConstant())
                        pResultArray[i] = this.mOutPorts[i].getConstantValue();
                    else
                        pResultArray[i] = ((SequenceOutPort) this.mOutPorts[i]).counter.increment();
                }
            }
        }
        else
            return COMPLETE;
        return 1;
    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new SequenceOutPort(this, srcStep);
    }

    @Override
    protected void close(boolean success) {        
    }
}
