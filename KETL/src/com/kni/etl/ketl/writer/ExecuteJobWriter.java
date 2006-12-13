/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Aug 9, 2006
 * 
 */
package com.kni.etl.ketl.writer;

import java.util.HashMap;
import java.util.HashSet;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class ExecuteJobWriter extends ETLWriter implements DefaultWriterCore {

    public ExecuteJobWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {

        String jobID;
        int projectID;
        boolean allowMultiple, ignoreDependencies;

        Object o = this.mInputNameMap.get("JOB_ID");

        if (o != null)
            o = this.mInPorts[(Integer) o].isConstant() ? this.mInPorts[(Integer) o].getConstantValue()
                    : pInputRecords[this.mInPorts[(Integer) o].getSourcePortIndex()];

        if (o == null)
            throw new KETLWriteException("JOB_ID cannot be NULL");

        jobID = o.toString();

        o = this.mInputNameMap.get("PROJECT_ID");
        if (o != null)
            o = this.mInPorts[(Integer) o].isConstant() ? this.mInPorts[(Integer) o].getConstantValue()
                    : pInputRecords[this.mInPorts[(Integer) o].getSourcePortIndex()];

        if (o == null)
            throw new KETLWriteException("PROJECT_ID cannot be NULL");

        if (o instanceof Number) {
            projectID = ((Number) o).intValue();
        }
        else {
            try {
                projectID = Integer.parseInt(o.toString());
            } catch (Exception e) {
                throw new KETLWriteException("Input for PROJECT_ID is invalid, " + o.toString());
            }
        }
        o = this.mInputNameMap.get("IGNOREDEPENDENCIES");
        if (o != null)
            o = this.mInPorts[(Integer) o].isConstant() ? this.mInPorts[(Integer) o].getConstantValue()
                    : pInputRecords[this.mInPorts[(Integer) o].getSourcePortIndex()];

        if (o == null)
            ignoreDependencies = false;
        else if (o instanceof Boolean) {
            ignoreDependencies = (Boolean) o;
        }
        else if (o instanceof CharSequence) {
            ignoreDependencies = Boolean.parseBoolean(((CharSequence) o).toString());
        }
        else
            throw new KETLWriteException("Input for IGNOREDEPENDENCIES is invalid, " + o.toString());

        o = this.mInputNameMap.get("ALLOWMULTIPLE");
        if (o != null)
            o = this.mInPorts[(Integer) o].isConstant() ? this.mInPorts[(Integer) o].getConstantValue()
                    : pInputRecords[this.mInPorts[(Integer) o].getSourcePortIndex()];

        if (o == null)
            allowMultiple = false;
        else if (o instanceof Boolean) {
            allowMultiple = (Boolean) o;
        }
        else if (o instanceof CharSequence) {
            allowMultiple = Boolean.parseBoolean(((CharSequence) o).toString());
        }
        else
            throw new KETLWriteException("Input for ALLOWMULTIPLE is invalid, " + o.toString());

        if (ResourcePool.getMetadata() == null)
            throw new KETLWriteException("Could not connect to the metadata");

        try {
            if (ResourcePool.getMetadata().executeJob(projectID, jobID, ignoreDependencies, allowMultiple) == false)
                throw new KETLWriteException("Failed to execute job, see KETLLog");
        } catch (Exception e) {
            throw new KETLWriteException("Error executing job - " + e.getMessage(), e);
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLStep#getRequiredTags()
     */
    @Override
    protected String[] getRequiredTags() {
        // TODO Auto-generated method stub
        return null;
    }

    private HashMap mInputNameMap = new HashMap();

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.writer.OpenETLWriter#initialize(org.w3c.dom.Node, java.util.HashMap)
     */
    @Override
    public int initialize(Node pXmlConfig) throws KETLThreadException {
        int res = super.initialize(pXmlConfig);

        if (res != 0)
            return res;

        HashSet hs = new HashSet();

        java.util.Collections
                .addAll(hs, new Object[] { "JOB_ID", "PROJECT_ID", "IGNOREDEPENDENCIES", "ALLOWMULTIPLE" });

        for (int i = 0; i < this.mInPorts.length; i++) {
            if (hs.contains(this.mInPorts[i].mstrName) == false) {
                ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Invalid input name of "
                        + this.mInPorts[i].mstrName + " will be ignored, it has to be one of "
                        + java.util.Arrays.toString(hs.toArray()));
            }
            this.mInputNameMap.put(this.mInPorts[i].mstrName, i);
        }
        return 0;
    }

    @Override
    protected void close(boolean success) {

    }

}
