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
 * Created on Jul 16, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl;

import java.math.BigDecimal;

import com.kni.util.ResizingArray;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLJobStatus.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class KETLJobStatus extends ETLJobStatus {

    /** The error count. */
    ResizingArray errorCount;
    
    /** The insert count. */
    ResizingArray insertCount;
    
    /** The update count. */
    ResizingArray updateCount;
    
    /** The batch count. */
    ResizingArray batchCount;
    
    /** The tmp BD. */
    BigDecimal tmpBD = new BigDecimal(0);
    
    /** The NO n_ PARTITIONED. */
    public static int NON_PARTITIONED = 0;
    
    /** The step ID. */
    int stepID = 0;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ETLStatus#getErrorMessage()
     */
    /**
     * The Class LongMutable.
     */
    class LongMutable {

        /** The value. */
        public long value = 0;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ETLStatus#getErrorMessage()
     */
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

    /**
     * Generate step identifier.
     * 
     * @return the int
     */
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

    /**
     * Increment error count.
     * 
     * @param pStep the step
     * @param pPartition the partition
     * @param pAmount the amount
     */
    public synchronized void incrementErrorCount(int pStep, int pPartition, long pAmount) {
        this.errorCount = this.incrementCount(pStep, pPartition, pAmount, this.errorCount);
    }

    /**
     * Increment insert count.
     * 
     * @param pStep the step
     * @param pPartition the partition
     * @param pAmount the amount
     */
    public synchronized void incrementInsertCount(int pStep, int pPartition, long pAmount) {
        this.insertCount = this.incrementCount(pStep, pPartition, pAmount, this.insertCount);
    }

    /**
     * Increment update count.
     * 
     * @param pStep the step
     * @param pPartition the partition
     * @param pAmount the amount
     */
    public synchronized void incrementUpdateCount(int pStep, int pPartition, long pAmount) {
        this.updateCount = this.incrementCount(pStep, pPartition, pAmount, this.updateCount);
    }

    /**
     * Increment batch count.
     * 
     * @param pStep the step
     * @param pPartition the partition
     * @param pAmount the amount
     */
    public synchronized void incrementBatchCount(int pStep, int pPartition, long pAmount) {
        this.batchCount = this.incrementCount(pStep, pPartition, pAmount, this.batchCount);
    }

    /**
     * Gets the XML synchronized.
     * 
     * @return the XML synchronized
     */
    public synchronized String getXMLSynchronized() {
        return null;
    }

    /**
     * Increment count.
     * 
     * @param pStep the step
     * @param pPartition the partition
     * @param pAmount the amount
     * @param pCounter the counter
     * 
     * @return the resizing array
     */
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

    /**
     * Gets the total count.
     * 
     * @param pCounter the counter
     * 
     * @return the total count
     */
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

    /**
     * Gets the total error count.
     * 
     * @return the total error count
     */
    public synchronized long getTotalErrorCount() {
        return this.getTotalCount(this.errorCount);
    }

    /**
     * Gets the total insert count.
     * 
     * @return the total insert count
     */
    public synchronized long getTotalInsertCount() {
        return this.getTotalCount(this.insertCount);
    }

    /**
     * Gets the total batch count.
     * 
     * @return the total batch count
     */
    public synchronized long getTotalBatchCount() {
        return this.getTotalCount(this.batchCount);
    }

    /**
     * Gets the total update count.
     * 
     * @return the total update count
     */
    public synchronized long getTotalUpdateCount() {
        return this.getTotalCount(this.updateCount);
    }
}
