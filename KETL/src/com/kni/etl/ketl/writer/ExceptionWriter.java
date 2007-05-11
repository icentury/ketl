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
package com.kni.etl.ketl.writer;

import java.util.HashMap;
import java.util.HashSet;

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;

// TODO: Auto-generated Javadoc
/**
 * The Class ExceptionWriter.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class ExceptionWriter extends ETLWriter implements DefaultWriterCore {

    /**
     * Instantiates a new exception writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public ExceptionWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        if (pPartition > 1) {
            throw new KETLThreadException(
                    "Exception writer cannot be run in parallel, or multiple exceptions will be generated, please set FLOWTYPE=\"FANIN\". Requested partitions: "
                            + pPartition, this);
        }
    }

    /** The input name map. */
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

        hs.add("MESSAGE");

        for (int i = 0; i < this.mInPorts.length; i++) {
            if (hs.contains(this.mInPorts[i].mstrName) == false) {
                com.kni.etl.dbutils.ResourcePool.LogMessage(this, com.kni.etl.dbutils.ResourcePool.INFO_MESSAGE,
                        "Invalid input name of " + this.mInPorts[i].mstrName + " will be ignored, it has to be one of "
                                + java.util.Arrays.toString(hs.toArray()));
            }
            this.mInputNameMap.put(this.mInPorts[i].mstrName, i);
        }

        if (this.mInputNameMap.containsKey("MESSAGE") == false)
            throw new KETLThreadException("MESSAGE input must be specified", this);

        return 0;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {

    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
     */
    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {
        String strMessage;

        ETLInPort port = this.mInPorts[(Integer) this.mInputNameMap.get("MESSAGE")];

        Object o = port.isConstant() ? port.getConstantValue() : pInputRecords[port.getSourcePortIndex()];

        if (o == null)
            throw new KETLWriteException("MESSAGE cannot be NULL");

        strMessage = o.toString();

        throw new ForcedException(strMessage);
    }

}
