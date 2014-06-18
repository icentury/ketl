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
/*
 * Created on Mar 17, 2003
 * 
 * To change this generated comment go to Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.w3c.dom.Node;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLReadException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.DefaultReaderCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class HelloWorld.
 */
public class SQSReader extends ETLReader implements DefaultReaderCore {

  @Override
  protected String getVersion() {
    return "$LastChangedRevision: 491 $";
  }

  /**
   * Instantiates a new hello world.
   * 
   * @param pXMLConfig the XML config
   * @param pPartitionID the partition ID
   * @param pPartition the partition
   * @param pThreadManager the thread manager
   * 
   * @throws KETLThreadException the KETL thread exception
   */
  public SQSReader(Node pXMLConfig, int pPartitionID, int pPartition,
      ETLThreadManager pThreadManager) throws KETLThreadException {
    super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
  }



  class SQSQueue {
    String endpoint;
    AmazonSQSClient sqsClient;
    public int waitTimeSeconds = 0;
    private List<DeleteMessageBatchRequestEntry> messagesToDelete = new ArrayList();
    public String name;

    public void recordMessageIDs(List<Message> messages) {
      for (Message m : messages)
        messagesToDelete.add(new DeleteMessageBatchRequestEntry(Integer.toString(messagesToDelete
            .size()), m.getReceiptHandle()));

    }

    public void deleteMessages() {
      while (messagesToDelete.size() > 0) {
        List<DeleteMessageBatchRequestEntry> batch =
            new ArrayList<DeleteMessageBatchRequestEntry>();

        setWaiting("messages to deleted from queue, " + messagesToDelete.size() + " left");

        while (batch.size() < 10 && messagesToDelete.size() > 0)
          batch.add(this.messagesToDelete.remove(0));

        if (batch.size() > 0)
          sqsClient.deleteMessageBatch(new DeleteMessageBatchRequest(this.endpoint, batch));
      }

      setWaiting(null);

    }



  }

  private List<SQSQueue> endPoints = new ArrayList<SQSQueue>();
  private long miMaxRuntime;

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
   */
  @Override
  public int initialize(Node pXmlConfig) throws KETLThreadException {
    int res = super.initialize(pXmlConfig);


    this.miMaxRuntime =
        XMLHelper.getAttributeAsInt(pXmlConfig.getAttributes(), "MAXRUNTIME", Integer.MAX_VALUE) * 1000;

    if (this.maParameters != null) {
      for (int paramList = 0; paramList < this.maParameters.size(); paramList++) {
        try {
          SQSQueue queue = new SQSQueue();
          queue.sqsClient =
              new AmazonSQSClient(new BasicAWSCredentials(this.getParameterValue(paramList,
                  "AWSKEY"), this.getParameterValue(paramList, "AWSSECRET")));

          String queueName = this.getParameterValue(paramList, "SQSQUEUENAME");
          int waitTimeSeconds =
              this.getParameterValue(paramList, "SQSWAITTIME") != null ? Integer.parseInt(this
                  .getParameterValue(paramList, "SQSWAITTIME")) : 0;

          if (queueName.contains("*")) {
            ListQueuesResult queueList = queue.sqsClient.listQueues(queueName.replace("*", ""));
            for (String endPoint : queueList.getQueueUrls()) {
              queue = new SQSQueue();
              queue.sqsClient =
                  new AmazonSQSClient(new BasicAWSCredentials(this.getParameterValue(paramList,
                      "AWSKEY"), this.getParameterValue(paramList, "AWSSECRET")));
              queue.endpoint = endPoint;
              queue.waitTimeSeconds = waitTimeSeconds;
              queue.name = endPoint.substring(endPoint.lastIndexOf('/') + 1);
              endPoints.add(queue);
            }
          } else {
            queue.waitTimeSeconds = waitTimeSeconds;
            queue.name = queueName;
            queue.endpoint = queue.sqsClient.getQueueUrl(queueName).getQueueUrl();
            endPoints.add(queue);
          }

          // prevent skew by shuffling to partition id
          java.util.Collections.shuffle(this.endPoints, new Random(this.partitionID));
        } catch (QueueDoesNotExistException e) {
          throw new KETLThreadException("Error connecting to queue", e);
        }
      }
    }


    return res;
  }

  // this isn't needed here but if you need attributes at the port level then
  // this is where you do them.
  /**
   * The Class HelloWorldOutPort.
   */
  private static final String QUEUENAME = "$QUEUENAME";

  class SQSOutPort extends ETLOutPort {

    private String attributeName;

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLPort#containsCode()
     */
    @Override
    public boolean containsCode() throws KETLThreadException {
      return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.ETLPort#initialize(org.w3c.dom.Node)
     */
    @Override
    public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
      int res = super.initialize(xmlConfig);
      if (res != 0)
        return res;

      this.part =
          MessagePart.valueOf(XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "PART",
              MessagePart.body.name()).toLowerCase());
      if (this.part == MessagePart.attribute) {
        this.attributeName =
            XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "ATTRIBUTENAME", null);
      }
      return 0;
    }

    /**
     * Instantiates a new hello world out port.
     * 
     * @param esOwningStep the es owning step
     * @param esSrcStep the es src step
     */
    public SQSOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
      super(esOwningStep, esSrcStep);
    }

    MessagePart part;

    public String getPart(Message message) {
      switch (part) {
        case body:
          return message.getBody();
        case attribute:
          return message.getAttributes().get(attributeName);
        case messageid:
          return message.getMessageId();
        case receipthandle:
          return message.getReceiptHandle();
        case queuename:
          return message.getAttributes().get(QUEUENAME);
        default:
          return null;
      }
    }
  }

  enum MessagePart {
    body, messageid, receipthandle, attribute, queuename
  };


  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#getNewOutPort(com.kni.etl.ketl.ETLStep)
   */
  @Override
  protected ETLOutPort getNewOutPort(ETLStep srcStep) {
    return new SQSOutPort(this, srcStep);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.DefaultReaderCore#getNextRecord(java.lang.Object[],
   * java.lang.Class[], int)
   */

  private List<Message> messages = java.util.Collections.synchronizedList(new ArrayList<Message>());
  private int deleteId = 0;
  private Long sqsReadStart = null;

  public int getNextRecord(Object[] pResultArray, Class[] pExpectedDataTypes, int pRecordWidth)
      throws KETLReadException {

    if (this.sqsReadStart == null)
      this.sqsReadStart = System.currentTimeMillis();

    long execTime = System.currentTimeMillis() - this.sqsReadStart;
    // as we are generating records not reading from a source we need to use
    // a counter
    if (messages.size() == 0 && execTime < this.miMaxRuntime) {
      for (SQSQueue activeQueue : this.endPoints) {

        if (messages.size() <= this.batchSize) {
          ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
          receiveMessageRequest.setWaitTimeSeconds(activeQueue.waitTimeSeconds);
          receiveMessageRequest.setQueueUrl(activeQueue.endpoint);
          receiveMessageRequest.setMaxNumberOfMessages(this.batchSize);
          ReceiveMessageResult res = activeQueue.sqsClient.receiveMessage(receiveMessageRequest);
          List<Message> queueMessages = res.getMessages();

          if (queueMessages != null) {
            for (Message m : queueMessages) {
              m.addAttributesEntry(QUEUENAME, activeQueue.name);
              messages.add(m);
            }
            activeQueue.recordMessageIDs(queueMessages);

          }
        }
      }

    }

    if (messages.size() == 0) {
      return DefaultReaderCore.COMPLETE;
    }

    Message message = messages.remove(0);

    // cycle through each port assigning the appropiate value if port
    // used
    for (int i = 0; i < this.mOutPorts.length; i++) {
      if (this.mOutPorts[i].isUsed()) {

        // if port contains constant then use constant
        if (this.mOutPorts[i].isConstant())
          pResultArray[i] = this.mOutPorts[i].getConstantValue();
        else
          pResultArray[i] = ((SQSOutPort) this.mOutPorts[i]).getPart(message);

      }
    }

    // return row count, should always be one for a reader
    return 1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
   */
  @Override
  protected void close(boolean success, boolean jobSuccess) {}

  @Override
  public int complete() throws KETLThreadException {
    // TODO Auto-generated method stub
    int res = super.complete();

    for (SQSQueue q : this.endPoints)
      q.deleteMessages();

    return res;
  }
}
