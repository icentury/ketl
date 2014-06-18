/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl.ketl.smp;

import java.util.concurrent.BlockingQueue;

import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;

// TODO: Auto-generated Javadoc
/**
 * The Class ETLMerge.
 * 
 * @author nwakefield To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public abstract class ETLMerge extends ETLStep implements
    KETLExceptionWrapper<KETLTransformException> {



  /** The queue. */
  ManagedBlockingQueue queue;

  /** The src queue right. */
  private ManagedBlockingQueue srcQueueRight;

  /** The src queue left. */
  private ManagedBlockingQueue srcQueueLeft;

  /** The right queue open. */
  private boolean leftQueueOpen = true, rightQueueOpen = true;

  /** The core. */
  private DefaultMergeCore core;

  /**
   * The Constructor.
   * 
   * @param pXMLConfig the XML config
   * @param pPartitionID the partition ID
   * @param pPartition the partition
   * @param pThreadManager the thread manager
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  public ETLMerge(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
      throws KETLThreadException {
    super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
  }

  @Override
  public void wrapException(Exception e) throws KETLTransformException {
    if (e instanceof KETLTransformException)
      throw (KETLTransformException) e;

    throw new KETLTransformException(e);

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#setCore(com.kni.etl.ketl.smp.DefaultCore)
   */
  @Override
  final void setCore(DefaultCore newCore) {
    this.core = (DefaultMergeCore) newCore;
  }

  /** The batch manager. */
  protected MergeBatchManager mBatchManager;

  /**
   * Close queue.
   * 
   * @param data the data
   * @param len the len
   * 
   * @throws InterruptedException the interrupted exception
   * @throws KETLTransformException the KETL transform exception
   */
  private void closeQueue(Object[][] data, int len) throws InterruptedException,
      KETLTransformException {

    if (len != data.length) {
      Object[][] newResult = new Object[len][];
      System.arraycopy(data, 0, newResult, 0, len);
      data = newResult;
    }

    if (this.mBatchManagement) {
      data = this.mBatchManager.finishBatch(data, len);
      len = data.length;
    }

    this.updateThreadStats(len);
    this.queue.put(data);
    this.queue.put(ETLWorker.ENDOBJ);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#setOutputRecordDataTypes(java.lang.Class[],
   * java.lang.String)
   */
  @Override
  void setOutputRecordDataTypes(Class[] pClassArray, String pChannel) {
    this.mExpectedOutputDataTypes = pClassArray;
    this.mOutputRecordWidth = this.mExpectedOutputDataTypes.length;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreHeader()
   */
  @Override
  protected CharSequence generateCoreHeader() {
    return " public class " + this.getCoreClassName() + " extends ETLMergeCore { ";
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#generateCoreImports()
   */
  @Override
  protected String generateCoreImports() {
    return super.generateCoreImports() + "import com.kni.etl.ketl.smp.ETLMergeCore;\n"
        + "import com.kni.etl.ketl.smp.ETLMerge;\n";
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#executeWorker()
   */
  @Override
  final protected void executeWorker() throws KETLTransformException, InterruptedException,
      KETLThreadException {

    Object[][] oLeftBatch = null, oRightBatch = null, res = new Object[this.batchSize][], freeBatch =
        null;
    int leftPos = 0, rightPos = 0, rightLen = 0, leftLen = 0, resultLength = 0, batchLength =
        this.batchSize;

    while (true) {
      this.interruptExecution();
      Object oLeft = null, oRight = null;
      if (leftPos == leftLen && this.leftQueueOpen) {
        oLeft = this.getSourceQueueLeft().take();
        if (oLeft == ETLWorker.ENDOBJ) {
          this.leftQueueOpen = false;
          oLeftBatch = null;

          if (this.rightQueueOpen == false) {
            this.closeQueue(res, resultLength);
            break;
          }
        } else {
          if (freeBatch == null || freeBatch.length < oLeftBatch.length)
            freeBatch = oLeftBatch;

          oLeftBatch = (Object[][]) oLeft;
          leftLen = oLeftBatch.length;
          leftPos = 0;
        }
      }
      if (rightPos == rightLen && this.rightQueueOpen) {
        oRight = this.getSourceQueueRight().take();
        if (oRight == ETLWorker.ENDOBJ) {
          this.rightQueueOpen = false;
          oRightBatch = null;
          if (this.leftQueueOpen == false) {
            this.closeQueue(res, resultLength);
            break;
          }
        } else {
          if (freeBatch == null || freeBatch.length < oRightBatch.length)
            freeBatch = oRightBatch;

          oRightBatch = (Object[][]) oRight;
          rightLen = oRightBatch.length;
          rightPos = 0;
        }
      }

      Object[] result = new Object[this.mOutputRecordWidth];
      try {
        if (this.timing)
          this.startTimeNano = System.nanoTime();
        int code =
            this.core.mergeRecord(this.leftQueueOpen ? oLeftBatch[leftPos] : null,
                this.mLeftExpectedInputDataTypes, this.mLeftInputRecordWidth,
                this.rightQueueOpen ? oRightBatch[rightPos] : null,
                this.mRightExpectedInputDataTypes, this.mRightInputRecordWidth, result,
                this.mExpectedOutputDataTypes, this.mOutputRecordWidth);
        if (this.timing)
          this.totalTimeNano += System.nanoTime() - this.startTimeNano;

        switch (code) {
          case DefaultMergeCore.SUCCESS_ADVANCE_BOTH:
            if (this.leftQueueOpen && this.rightQueueOpen) {
              leftPos++;
              rightPos++;
              res[resultLength++] = result;
            } else
              throw new KETLTransformException(
                  "Cannot advance to next left or right record, invalid request", code);
            break;
          case DefaultMergeCore.SUCCESS_ADVANCE_LEFT:
            if (this.leftQueueOpen) {
              leftPos++;
              res[resultLength++] = result;
            } else
              throw new KETLTransformException(
                  "Cannot advance to next left record, invalid request", code);
            break;
          case DefaultMergeCore.SUCCESS_ADVANCE_RIGHT:
            if (this.rightQueueOpen) {
              rightPos++;
              res[resultLength++] = result;
            } else
              throw new KETLTransformException(
                  "Cannot advance to next right record, invalid request", code);
            break;
          case DefaultMergeCore.SKIP_ADVANCE_BOTH:
            if (this.leftQueueOpen && this.rightQueueOpen) {
              leftPos++;
              rightPos++;
            } else
              throw new KETLTransformException(
                  "Cannot advance to next left or right record, invalid request", code);
            break;
          case DefaultMergeCore.SKIP_ADVANCE_LEFT:
            if (this.leftQueueOpen)
              leftPos++;
            else
              throw new KETLTransformException(
                  "Cannot advance to next left record, invalid request", code);
            break;
          case DefaultMergeCore.SKIP_ADVANCE_RIGHT:
            if (this.rightQueueOpen)
              rightPos++;
            else
              throw new KETLTransformException(
                  "Cannot advance to next right record, invalid request", code);
            break;

          default:
            throw new KETLTransformException("Invalid return code, check previous error message",
                code);
        }
      } catch (KETLTransformException e) {
        try {
          this.incrementErrorCount(e, this.leftQueueOpen ? oLeftBatch[leftPos] : null,
              this.rightQueueOpen ? oRightBatch[rightPos] : null, -1);
        } catch (KETLException e1) {
          wrapException(e1);
        }
      }
      if (resultLength == batchLength) {

        if (this.mBatchManagement) {
          res = this.mBatchManager.finishBatch(res, res.length);
        }

        this.queue.put(res);
        this.updateThreadStats(res.length);
        resultLength = 0;
        if (freeBatch == null) {
          res = new Object[this.batchSize][];
        } else
          res = freeBatch;

        freeBatch = null;
        batchLength = res.length;
      }
    }

  }

  /** The expected output data types. */
  private Class[] mLeftExpectedInputDataTypes, mRightExpectedInputDataTypes,
      mExpectedOutputDataTypes;

  /** The output record width. */
  private int mLeftInputRecordWidth = -1, mRightInputRecordWidth = -1, mOutputRecordWidth = -1;

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.transform.ETLSourceQueue#getSourceQueue()
   */
  /**
   * Gets the left source queue.
   * 
   * @return the left source queue
   */
  final public BlockingQueue getLeftSourceQueue() {
    return this.getSourceQueueLeft();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.transform.ETLSourceQueue#getSourceQueue()
   */
  /**
   * Gets the right source queue.
   * 
   * @return the right source queue
   */
  final public BlockingQueue getRightSourceQueue() {
    return this.getSourceQueueRight();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.ETLWorker#initializeQueues(com.kni.etl.ketl.ETLWorker[],
   * com.kni.etl.ketl.ETLWorker[])
   */
  @Override
  final public void initializeQueues() {
    this.queue.registerWriter(this);
    this.getSourceQueueLeft().registerReader(this);
    this.getSourceQueueRight().registerReader(this);
  }


  /*
   * (non-Javadoc)
   * 
   * @see
   * com.kni.etl.ketl.smp.ETLWorker#switchTargetQueue(com.kni.etl.ketl.smp.ManagedBlockingQueue,
   * com.kni.etl.ketl.smp.ManagedBlockingQueue)
   */
  @Override
  public void switchTargetQueue(ManagedBlockingQueue currentQueue, ManagedBlockingQueue newQueue) {
    this.queue = newQueue;
  }


  /**
   * Sets the source queue left.
   * 
   * @param srcQueueLeft the src queue left
   * @param worker the worker
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  void setSourceQueueLeft(ManagedBlockingQueue srcQueueLeft, ETLWorker worker)
      throws KETLThreadException {
    this.getUsedPortsFromWorker(worker, ETLWorker.getChannel(this.getXMLConfig(), ETLWorker.LEFT),
        ETLWorker.LEFT, "pLeftInputRecords");
    this.srcQueueLeft = srcQueueLeft;
    try {
      this.mLeftExpectedInputDataTypes =
          worker
              .getOutputRecordDatatypes(ETLWorker.getChannel(this.getXMLConfig(), ETLWorker.LEFT));
      this.mLeftInputRecordWidth = this.mLeftExpectedInputDataTypes.length;
    } catch (ClassNotFoundException e) {
      throw new KETLThreadException(e, this);
    }

  }

  /**
   * Gets the source queue left.
   * 
   * @return the source queue left
   */
  ManagedBlockingQueue getSourceQueueLeft() {
    return this.srcQueueLeft;
  }

  /**
   * Sets the source queue right.
   * 
   * @param srcQueueRight the src queue right
   * @param worker the worker
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  void setSourceQueueRight(ManagedBlockingQueue srcQueueRight, ETLWorker worker)
      throws KETLThreadException {
    this.getUsedPortsFromWorker(worker, ETLWorker.getChannel(this.getXMLConfig(), ETLWorker.RIGHT),
        ETLWorker.RIGHT, "pRightInputRecords");
    this.srcQueueRight = srcQueueRight;
    try {
      this.mRightExpectedInputDataTypes =
          worker
              .getOutputRecordDatatypes(ETLWorker.getChannel(this.getXMLConfig(), ETLWorker.RIGHT));
      this.mRightInputRecordWidth = this.mRightExpectedInputDataTypes.length;
    } catch (ClassNotFoundException e) {
      throw new KETLThreadException(e, this);
    }
  }

  /**
   * Gets the used ports from worker.
   * 
   * @param pWorker the worker
   * @param port the port
   * @param type the type
   * @param objectNameInCode the object name in code
   * 
   * @return the used ports from worker
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  protected void getUsedPortsFromWorker(ETLWorker pWorker, String port, int type,
      String objectNameInCode) throws KETLThreadException {

    ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Registering port usage for step "
        + pWorker.toString() + ":" + (type == ETLWorker.LEFT ? "LEFT" : "RIGHT") + " by step "
        + this.toString());

    Node[] nl =
        com.kni.etl.util.XMLHelper.getElementsByName(this.getXMLConfig(), "IN",
            type == ETLWorker.LEFT ? "LEFT" : "RIGHT", "TRUE");
    this.registerUsedPorts(pWorker, nl, objectNameInCode);

  }

  /**
   * Gets the source queue right.
   * 
   * @return the source queue right
   */
  ManagedBlockingQueue getSourceQueueRight() {

    return this.srcQueueRight;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getRecordExecuteMethodHeader()
   */
  @Override
  protected String getRecordExecuteMethodHeader() {
    StringBuilder sb = new StringBuilder();

    sb.append("public int mergeRecord(Object[] pLeftInputRecords, Class[] pLeftInputDataTypes,"
        + " int pLeftInputRecordWidth, Object[] pRightInputRecords, "
        + " Class[] pRightInputDataTypes, int pRightInputRecordWidth,"
        + " Object[] pOutputRecords "
        + ", Class[] pOutputDataTypes, int pOutputRecordWidth) throws KETLTransformException {");

    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getRecordExecuteMethodFooter()
   */
  @Override
  protected String getRecordExecuteMethodFooter() {
    return " return -1;}";
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getDefaultExceptionClass()
   */
  @Override
  final String getDefaultExceptionClass() {
    return KETLTransformException.class.getCanonicalName();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#setBatchManager(com.kni.etl.ketl.smp.BatchManager)
   */
  @Override
  final protected void setBatchManager(BatchManager batchManager) {
    this.mBatchManager = (MergeBatchManager) batchManager;
  }
}
