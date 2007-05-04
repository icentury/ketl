/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import org.w3c.dom.Node;

import com.kni.etl.SharedCounter;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

/**
 * <p>
 * Title: ETLWriter
 * </p>
 * <p>
 * Description: Abstract base class for ETL destination loading.
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class NullWriter extends ETLWriter implements DefaultWriterCore {

    long mReportBack = 100000;
    private String LOGEVERY_ATTRIB = "LOGEVERY";
    private String LOG_ATTRIB = "LOG";
    private boolean mLog = true;

    public NullWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {
        int res = super.initialize(xmlConfig);
        if (res != 0)
            return res;

        this.mReportBack = XMLHelper.getAttributeAsInt(xmlConfig.getAttributes(), this.LOGEVERY_ATTRIB,
                (int) this.mReportBack);
        this.mLog = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), this.LOG_ATTRIB, true);

        this.mSharedCounter = this.getJobExecutor().getCurrentETLJob().getCounter(this.getName());
        return res;
    }

    SharedCounter mSharedCounter;

    public int putNextRecord(Object[] o, Class[] pExpectedDataTypes, int pRecordWidth) throws KETLWriteException {

        if (this.mLog) {
            if (this.mSharedCounter.increment(1) % this.mReportBack == 0)
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Records processed: "
                        + this.mSharedCounter.value());
        }
        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLStep#complete()
     */
    @Override
    public int complete() throws KETLThreadException {
        int res = super.complete();

        if (this.mLog && this.isLastThreadToEnterCompletePhase())
            ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Total Records processed: "
                    + this.mSharedCounter.value());
        return res;
    }

    @Override
    protected void close(boolean success) {

    }

}
