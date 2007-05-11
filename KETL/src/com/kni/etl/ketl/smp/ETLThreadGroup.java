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
package com.kni.etl.ketl.smp;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLThreadGroup.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class ETLThreadGroup {

    /** The m queue. */
    private ManagedBlockingQueue[] mQueue;
    
    /** The m ETL workers. */
    private ETLWorker[] mETLWorkers;
    
    /** The Constant FANOUT. */
    public final static int FANOUT = 0;
    
    /** The Constant PIPELINE. */
    public final static int PIPELINE = 2;
    
    /** The Constant PIPELINE_MERGE. */
    public final static int PIPELINE_MERGE = 4;
    
    /** The Constant PIPELINE_SPLIT. */
    public final static int PIPELINE_SPLIT = 5;
    
    /** The Constant FANOUTIN. */
    public final static int FANOUTIN = 1;
    
    /** The Constant FANIN. */
    public final static int FANIN = 3;
    
    /** The m ETL thread manager. */
    private ETLThreadManager mETLThreadManager;
    
    /** The m queue size. */
    private int mQueueSize;
    
    /** The Constant DEFAULTQUEUESIZE. */
    private final static int DEFAULTQUEUESIZE = 5;
    
    /** The m port list. */
    private String[] mPortList = null;

    /**
     * New instance.
     * 
     * @param srcGrp the src grp
     * @param iType the i type
     * @param type the type
     * @param partitions the partitions
     * @param pThreadManager the thread manager
     * 
     * @return the ETL thread group
     * 
     * @throws SecurityException the security exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws NoSuchMethodException the no such method exception
     * @throws InvocationTargetException the invocation target exception
     * @throws KETLThreadException the KETL thread exception
     */
    public static ETLThreadGroup newInstance(ETLThreadGroup srcGrp, int iType, Step type, int partitions,
            ETLThreadManager pThreadManager) throws SecurityException, IllegalArgumentException,
            InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
            KETLThreadException {
        return new ETLThreadGroup(srcGrp, iType, type, partitions, pThreadManager);
    }

    /**
     * New instance.
     * 
     * @param srcLeftGrp the src left grp
     * @param srcRightGrp the src right grp
     * @param iType the i type
     * @param type the type
     * @param partitions the partitions
     * @param pThreadManager the thread manager
     * 
     * @return the ETL thread group
     * 
     * @throws SecurityException the security exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws NoSuchMethodException the no such method exception
     * @throws InvocationTargetException the invocation target exception
     * @throws KETLThreadException the KETL thread exception
     */
    public static ETLThreadGroup newInstance(ETLThreadGroup srcLeftGrp, ETLThreadGroup srcRightGrp, int iType,
            Step type, int partitions, ETLThreadManager pThreadManager) throws SecurityException,
            IllegalArgumentException, InstantiationException, IllegalAccessException, NoSuchMethodException,
            InvocationTargetException, KETLThreadException {
        return new ETLThreadGroup(srcLeftGrp, srcRightGrp, iType, type, partitions, pThreadManager);
    }

    /**
     * New instances.
     * 
     * @param srcGrp the src grp
     * @param pPorts the ports
     * @param iType the i type
     * @param type the type
     * @param partitions the partitions
     * @param pThreadManager the thread manager
     * 
     * @return the ETL thread group[]
     * 
     * @throws SecurityException the security exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws NoSuchMethodException the no such method exception
     * @throws InvocationTargetException the invocation target exception
     * @throws KETLThreadException the KETL thread exception
     */
    public static ETLThreadGroup[] newInstances(ETLThreadGroup srcGrp, String[] pPorts, int iType, Step type,
            int partitions, ETLThreadManager pThreadManager) throws SecurityException, IllegalArgumentException,
            InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
            KETLThreadException {

        if (iType != ETLThreadGroup.PIPELINE_SPLIT || ETLSplit.class.isAssignableFrom(type.getNodeClass()) == false)
            throw new KETLThreadException("Invalid type supplied", Thread.currentThread());

        ETLThreadGroup[] grp = new ETLThreadGroup[pPorts.length];

        for (int i = 0; i < pPorts.length; i++) {
            grp[i] = new ETLThreadGroup();
            grp[i].mPortList = pPorts;
            grp[i].mQueueSize = XMLHelper.getAttributeAsInt(type.getConfig().getAttributes(), "QUEUESIZE",
                    ETLThreadGroup.DEFAULTQUEUESIZE);
            grp[i].mETLThreadManager = pThreadManager;
            grp[i].mQueue = new ManagedBlockingQueue[srcGrp.mQueue.length];

            for (int q = 0; q < grp[i].mQueue.length; q++)
                grp[i].mQueue[q] = grp[i].getManagedQueue();
        }

        ETLWorker[] workers = new ETLWorker[srcGrp.mQueue.length];

        for (int i = 0; i < workers.length; i++) {
            Constructor cons = type.getNodeClass().getConstructor(
                    new Class[] { Node.class, int.class, int.class, ETLThreadManager.class });
            workers[i] = (ETLWorker) cons.newInstance(new Object[] { type.getConfig(), i, workers.length,
                    pThreadManager });

            if (workers[i] instanceof ETLSplit) {
                ((ETLSplit) workers[i]).queue = new ManagedBlockingQueue[pPorts.length];

                for (int p = 0; p < pPorts.length; p++) {
                    ((ETLSplit) workers[i]).queue[p] = grp[p].mQueue[i];
                    grp[p].mETLWorkers = workers;
                }
                ((ETLSplit) workers[i]).setSourceQueue(srcGrp.mQueue[i], srcGrp.mETLWorkers[i]);
                srcGrp.mETLWorkers[i].postSourceConnectedInitialize();
            }

        }

        return grp;
    }

    /**
     * Instantiates a new ETL thread group.
     * 
     * @param srcGrp the src grp
     * @param iType the i type
     * @param type the type
     * @param partitions the partitions
     * @param pThreadManager the thread manager
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws SecurityException the security exception
     * @throws NoSuchMethodException the no such method exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws InvocationTargetException the invocation target exception
     * @throws KETLThreadException the KETL thread exception
     */
    public ETLThreadGroup(ETLThreadGroup srcGrp, int iType, Step type, int partitions, ETLThreadManager pThreadManager)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException,
            IllegalArgumentException, InvocationTargetException, KETLThreadException {
        super();

        this.mQueueSize = XMLHelper.getAttributeAsInt(type.getConfig().getAttributes(), "QUEUESIZE", ETLThreadGroup.DEFAULTQUEUESIZE);
        this.mETLThreadManager = pThreadManager;

        if (iType == ETLThreadGroup.FANOUT) {

            if (srcGrp != null && srcGrp.mETLWorkers.length > 1) {
                if (srcGrp.mETLWorkers.length != partitions) {
                    com.kni.etl.dbutils.ResourcePool
                            .LogMessage(this, com.kni.etl.dbutils.ResourcePool.INFO_MESSAGE,
                                    "Source thread group is already partitioned, matching source partition count and switching to pipelined parrallism");
                    partitions = srcGrp.mETLWorkers.length;
                }
                else {
                    com.kni.etl.dbutils.ResourcePool.LogMessage(this, com.kni.etl.dbutils.ResourcePool.INFO_MESSAGE,
                            "Source thread group is already partitioned, switching to pipelined parrallism");
                }
                iType = ETLThreadGroup.PIPELINE;
            }
        }

        switch (iType) {
        case PIPELINE:
            if (srcGrp == null) {
                this.mETLWorkers = new ETLWorker[partitions];
                this.mQueue = new ManagedBlockingQueue[partitions];
            }
            else {
                this.mETLWorkers = new ETLWorker[srcGrp.mQueue.length];
                this.mQueue = new ManagedBlockingQueue[srcGrp.mQueue.length];
            }

            for (int i = 0; i < this.mETLWorkers.length; i++) {
                Constructor cons = type.getNodeClass().getConstructor(
                        new Class[] { Node.class, int.class, int.class, ETLThreadManager.class });
                this.mETLWorkers[i] = (ETLWorker) cons.newInstance(new Object[] { type.getConfig(), i, this.mETLWorkers.length,
                        pThreadManager });

                this.mQueue[i] = this.getManagedQueue();
                if (this.mETLWorkers[i] instanceof ETLWriter) {

                    ((ETLWriter) this.mETLWorkers[i]).setSourceQueue(srcGrp.mQueue[i], srcGrp.mETLWorkers[i]);
                    srcGrp.mETLWorkers[i].postSourceConnectedInitialize();
                }
                if (this.mETLWorkers[i] instanceof ETLReader) {
                    ((ETLReader) this.mETLWorkers[i]).queue = this.mQueue[i];
                }
                if (this.mETLWorkers[i] instanceof ETLTransform) {
                    ((ETLTransform) this.mETLWorkers[i]).queue = this.mQueue[i];
                    ((ETLTransform) this.mETLWorkers[i]).setSourceQueue(srcGrp.mQueue[i], srcGrp.mETLWorkers[i]);
                    srcGrp.mETLWorkers[i].postSourceConnectedInitialize();
                }

            }

            break;
        case FANIN:
            this.mETLWorkers = new ETLWorker[1];
            this.mQueue = new ManagedBlockingQueue[1];
            this.mQueue[0] = this.getManagedQueue();

            ManagedBlockingQueue q = srcGrp.mQueue[0];
            for (ETLWorker element : srcGrp.mETLWorkers) {

                    // set source to use single queue
                    if (element instanceof ETLReader) {
                        ((ETLReader) element).queue = q;
                    }
                    if (element instanceof ETLTransform) {
                        ((ETLTransform) element).queue = q;
                    }

                    srcGrp.mQueue = new ManagedBlockingQueue[1];
                    srcGrp.mQueue[0] = q;
                }
            {
                Constructor cons = type.getNodeClass().getConstructor(
                        new Class[] { Node.class, int.class, int.class, ETLThreadManager.class });
                this.mETLWorkers[0] = (ETLWorker) cons.newInstance(new Object[] { type.getConfig(), 0,
                        srcGrp.mQueue.length, pThreadManager });

                for (ETLWorker element : srcGrp.mETLWorkers) {
                    if (element instanceof ETLWriter) {
                        ((ETLWriter) element).setSourceQueue(q, element);
                    }
                    if (element instanceof ETLTransform) {
                        this.mQueue[0] = this.getManagedQueue();

                        ((ETLTransform) element).queue = this.mQueue[0];
                        ((ETLTransform) element).setSourceQueue(q, element);
                    }

                    element.postSourceConnectedInitialize();
                }
            }

            break;
        case FANOUT:
            this.mETLWorkers = new ETLWorker[partitions];

            this.mQueue = new ManagedBlockingQueue[partitions];
            Partitioner partitioningQueue = this.getPartitioner(type.getConfig(), partitions);

            for (int partition = 0; partition < partitions; partition++) {
                Constructor cons = type.getNodeClass().getConstructor(
                        new Class[] { Node.class, int.class, int.class, ETLThreadManager.class });
                this.mETLWorkers[partition] = (ETLWorker) cons.newInstance(new Object[] { type.getConfig(),
                        partition, partitions, pThreadManager });

                if (this.mETLWorkers[partition] instanceof ETLReader) {
                    this.mQueue[partition] = this.getManagedQueue();
                    ((ETLReader) this.mETLWorkers[partition]).queue = this.mQueue[partition];

                }
                else {

                    /*
                     * if the source is not partitioned and the destination request default partitioning then get the
                     * source queue
                     */

                    ETLWorker srcWorker = srcGrp.mETLWorkers[0];
                    ManagedBlockingQueue srcQueue = srcGrp.mQueue[0];

                    if (partitioningQueue != null) {
                        srcWorker.switchTargetQueue(srcGrp.mQueue[0], partitioningQueue);
                        srcGrp.mQueue[0] = partitioningQueue;
                        srcQueue = partitioningQueue.getTargetSourceQueue(partition);
                    }

                    if (this.mETLWorkers[partition] instanceof ETLTransform) {
                        this.mQueue[partition] = this.getManagedQueue();

                        ((ETLTransform) this.mETLWorkers[partition]).setSourceQueue(srcQueue, srcWorker);
                        ((ETLTransform) this.mETLWorkers[partition]).queue = this.mQueue[partition];

                    }
                    if (this.mETLWorkers[partition] instanceof ETLWriter) {
                        ((ETLWriter) this.mETLWorkers[partition]).setSourceQueue(srcQueue, srcWorker);
                        this.mQueue = null;
                    }
                }
            }

            if (srcGrp != null)
                srcGrp.mETLWorkers[0].postSourceConnectedInitialize();

            break;
        }
    }

    /**
     * Gets the partitioner.
     * 
     * @param xmlNode the xml node
     * @param targetPartitions the target partitions
     * 
     * @return the partitioner
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    private Partitioner getPartitioner(Node xmlNode, int targetPartitions) throws KETLThreadException {
        Node[] partitionKeys = XMLHelper.getElementsByName(xmlNode, "IN", "PARTITIONKEY", null);

        Node[] sortKeys = XMLHelper.getElementsByName(xmlNode, "IN", "BUFFERSORT", null);
        Comparator comp = null;

        if (sortKeys != null && sortKeys.length > 0) {
            Integer[] elements = new Integer[sortKeys.length];
            Boolean[] elementOrder = new Boolean[sortKeys.length];

            for (int i = 0; i < sortKeys.length; i++) {
                elements[i] = XMLHelper.getAttributeAsInt(sortKeys[i].getAttributes(), "BUFFERSORT", 0);
                elementOrder[i] = XMLHelper.getAttributeAsBoolean(sortKeys[i].getAttributes(), "BUFFERSORTORDER", true);                
            }
            comp = new DefaultComparator(elements, elementOrder);

            if (partitionKeys == null || partitionKeys.length == 0)
                return null;
        }

        int[] indexCheck = new int[partitionKeys.length];
        java.util.Arrays.fill(indexCheck, -1);
        for (Node element : partitionKeys) {
            int id = XMLHelper.getAttributeAsInt(element.getAttributes(), "PARTITIONKEY", -1);
            if (id <= indexCheck.length && id > 0) {
                indexCheck[id - 1] = 0;
            }
            else
                throw new KETLThreadException("Invalid PARTITIONKEY value", this);
        }

        for (int element : indexCheck) {
            if (element == -1)
                throw new KETLThreadException("Invalid PARTITIONKEY settings, key sequence order is wrong", this);
        }

        return new Partitioner(partitionKeys, comp, targetPartitions, this.mQueueSize);
    }

    /**
     * Instantiates a new ETL thread group.
     */
    private ETLThreadGroup() {
        super();
    }

    /**
     * Instantiates a new ETL thread group.
     * 
     * @param srcGrp the src grp
     * @param pPaths the paths
     * @param iType the i type
     * @param type the type
     * @param partitions the partitions
     * @param pThreadManager the thread manager
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws SecurityException the security exception
     * @throws NoSuchMethodException the no such method exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws InvocationTargetException the invocation target exception
     * @throws KETLThreadException the KETL thread exception
     */
    public ETLThreadGroup(ETLThreadGroup srcGrp, int pPaths, int iType, Step type, int partitions,
            ETLThreadManager pThreadManager) throws InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException, IllegalArgumentException, InvocationTargetException, KETLThreadException {
        super();

        this.mQueueSize = XMLHelper.getAttributeAsInt(type.getConfig().getAttributes(), "QUEUESIZE", ETLThreadGroup.DEFAULTQUEUESIZE);

        this.mETLThreadManager = pThreadManager;

        switch (iType) {
        case PIPELINE_SPLIT:
            if (srcGrp == null) {
                this.mETLWorkers = new ETLWorker[partitions];
                this.mQueue = new ManagedBlockingQueue[partitions];
            }
            else {
                this.mETLWorkers = new ETLWorker[srcGrp.mQueue.length];
                this.mQueue = new ManagedBlockingQueue[srcGrp.mQueue.length];
            }

            for (int i = 0; i < this.mETLWorkers.length; i++) {
                Constructor cons = type.getNodeClass().getConstructor(
                        new Class[] { Node.class, int.class, int.class, ETLThreadManager.class });
                this.mETLWorkers[i] = (ETLWorker) cons.newInstance(new Object[] { type.getConfig(), i,
                        this.mETLWorkers.length, pThreadManager });

                this.mQueue[i] = this.getManagedQueue();

                if (this.mETLWorkers[i] instanceof ETLSplit) {
                    ((ETLSplit) this.mETLWorkers[i]).queue[pPaths] = this.mQueue[i];
                    ((ETLSplit) this.mETLWorkers[i]).setSourceQueue(srcGrp.mQueue[i], srcGrp.mETLWorkers[i]);
                }

                srcGrp.mETLWorkers[i].postSourceConnectedInitialize();

            }

            break;

        }
    }

    /**
     * Instantiates a new ETL thread group.
     * 
     * @param srcLeftGrp the src left grp
     * @param srcRightGrp the src right grp
     * @param iType the i type
     * @param type the type
     * @param partitions the partitions
     * @param pThreadManager the thread manager
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws SecurityException the security exception
     * @throws NoSuchMethodException the no such method exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws InvocationTargetException the invocation target exception
     * @throws KETLThreadException the KETL thread exception
     */
    public ETLThreadGroup(ETLThreadGroup srcLeftGrp, ETLThreadGroup srcRightGrp, int iType, Step type, int partitions,
            ETLThreadManager pThreadManager) throws InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException, IllegalArgumentException, InvocationTargetException, KETLThreadException {
        super();

        this.mQueueSize = XMLHelper.getAttributeAsInt(type.getConfig().getAttributes(), "QUEUESIZE", ETLThreadGroup.DEFAULTQUEUESIZE);

        this.mETLThreadManager = pThreadManager;

        switch (iType) {
        case PIPELINE_MERGE:
            if (srcLeftGrp.mQueue.length != srcRightGrp.mQueue.length)
                throw new KETLThreadException("Left and right sources must have the same parallism", this);
            this.mETLWorkers = new ETLMerge[srcLeftGrp.mQueue.length];
            this.mQueue = new ManagedBlockingQueue[srcLeftGrp.mQueue.length];

            for (int i = 0; i < this.mETLWorkers.length; i++) {
                Constructor cons = type.getNodeClass().getConstructor(
                        new Class[] { Node.class, int.class, int.class, ETLThreadManager.class });
                this.mETLWorkers[i] = (ETLWorker) cons.newInstance(new Object[] { type.getConfig(), i,
                        this.mETLWorkers.length, pThreadManager });

                this.mQueue[i] = this.getManagedQueue();
                if (this.mETLWorkers[i] instanceof ETLMerge) {
                    ((ETLMerge) this.mETLWorkers[i]).queue = this.mQueue[i];
                    ((ETLMerge) this.mETLWorkers[i]).setSourceQueueLeft(srcLeftGrp.mQueue[i], srcLeftGrp.mETLWorkers[i]);
                    ((ETLMerge) this.mETLWorkers[i]).setSourceQueueRight(srcRightGrp.mQueue[i], srcRightGrp.mETLWorkers[i]);
                }

                srcLeftGrp.mETLWorkers[i].postSourceConnectedInitialize();
                srcRightGrp.mETLWorkers[i].postSourceConnectedInitialize();

            }

            break;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.transform.ETLSourceQueue#getSourceQueue()
     */
    /**
     * Gets the managed queue.
     * 
     * @return the managed queue
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    final public ManagedBlockingQueue getManagedQueue() throws KETLThreadException {
        return this.mETLThreadManager.requestQueue(this.mQueueSize);
    }

    /**
     * Gets the port name.
     * 
     * @param i the i
     * 
     * @return the port name
     */
    public String getPortName(int i) {
        // TODO Auto-generated method stub
        if (this.mPortList == null)
            return "DEFAULT";
        return this.mPortList[i];
    }

    /**
     * Sets the queue name.
     * 
     * @param port the new queue name
     */
    public void setQueueName(String port) {
        for (ManagedBlockingQueue element : this.mQueue)
            element.setName(port);

    }

}
