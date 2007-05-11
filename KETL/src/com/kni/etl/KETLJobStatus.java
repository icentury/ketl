/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 16, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl;

import java.math.BigDecimal;

import com.kni.util.ResizingArray;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class KETLJobStatus extends ETLJobStatus {

    ResizingArray errorCount;
    ResizingArray insertCount;
    ResizingArray updateCount;
    ResizingArray batchCount;
    BigDecimal tmpBD = new BigDecimal(0);
    public static int NON_PARTITIONED = 0;
    int stepID = 0;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLStatus#getErrorMessage()
     */
    class LongMutable {

        public long value = 0;
    }

    @Override
    public synchronized String getErrorMessage() {
        return super.getErrorMessage();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLStatus#getStatusMessage()
     */
    @Override
    public String getStatusMessage() {
        return super.getStatusMessage();
    }

    public synchronized int generateStepIdentifier() {
        return this.stepID++;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLStatus#getStatusMessageForCode(int)
     */
    @Override
    public String getStatusMessageForCode(int iStatusCode) {
        return super.getStatusMessageForCode(iStatusCode);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLStatus#getWarningMessage()
     */
    @Override
    public synchronized String getWarningMessage() {
        return super.getWarningMessage();
    }

    public synchronized void incrementErrorCount(int pStep, int pPartition, long pAmount) {
        this.errorCount = this.incrementCount(pStep, pPartition, pAmount, this.errorCount);
    }

    public synchronized void incrementInsertCount(int pStep, int pPartition, long pAmount) {
        this.insertCount = this.incrementCount(pStep, pPartition, pAmount, this.insertCount);
    }

    public synchronized void incrementUpdateCount(int pStep, int pPartition, long pAmount) {
        this.updateCount = this.incrementCount(pStep, pPartition, pAmount, this.updateCount);
    }

    public synchronized void incrementBatchCount(int pStep, int pPartition, long pAmount) {
        this.batchCount = this.incrementCount(pStep, pPartition, pAmount, this.batchCount);
    }

    public synchronized String getXMLSynchronized() {
        return null;
    }

    private ResizingArray incrementCount(int pStep, int pPartition, long pAmount, ResizingArray pCounter) {
        // step counters not defined create it for all steps
        if (pCounter == null) {
            pCounter = new ResizingArray(pStep + 1);
        }

        // get step array with step counters
        ResizingArray pStepArray = (ResizingArray) pCounter.get(pStep);

        // if step not defined then create step array
        if (pStepArray == null) {
            pCounter.add(pStep, new ResizingArray(pPartition + 1));
            pStepArray = (ResizingArray) pCounter.get(pStep);
        }

        // get counter for partition within step
        LongMutable val = (LongMutable) pStepArray.get(pPartition);

        // not defined create counter for partition within step
        if (val == null) {
            val = new LongMutable();
            pStepArray.add(pPartition, val);
        }

        val.value = val.value + pAmount;

        return pCounter;
    }

    private long getTotalCount(ResizingArray pCounter) {
        // step counters not defined create it for all steps
        if (pCounter == null) {
            return 0;
        }

        long res = 0;

        // get step array with step counters
        for (int i = 0; i < pCounter.size(); i++) {
            ResizingArray pStepArray = (ResizingArray) pCounter.get(i);

            if (pStepArray != null) {
                for (int p = 0; p < pStepArray.size(); p++) {
                    LongMutable val = (LongMutable) pStepArray.get(p);

                    if (val != null) {
                        res = res + val.value;
                    }
                }
            }
        }

        return res;
    }

    public synchronized long getTotalErrorCount() {
        return this.getTotalCount(this.errorCount);
    }

    public synchronized long getTotalInsertCount() {
        return this.getTotalCount(this.insertCount);
    }

    public synchronized long getTotalBatchCount() {
        return this.getTotalCount(this.batchCount);
    }

    public synchronized long getTotalUpdateCount() {
        return this.getTotalCount(this.updateCount);
    }
}
