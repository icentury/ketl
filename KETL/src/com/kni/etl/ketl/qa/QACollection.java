/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 8, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.qa;

import java.util.ArrayList;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.Metadata;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLEvent;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

/**
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public class QACollection extends QA {

    public static String QA = "QA";
    QAItemLevelEventGenerator aItemsToCheck[] = null;
    QAInitializeLevelEventGenerator[] initializeLevel = null;
    QARecordLevelEventGenerator[] recordLevel = null;

    /**
     * @param eStep
     * @param nXMLConfig
     */
    public QACollection(ETLStep eStep, Node nXMLConfig) {
        super();

        this.nQADefinition = nXMLConfig;
        this.step = eStep;

        // inialize all objects
        // get Step QA items
        String sQAName = XMLHelper.getAttributeAsString(nXMLConfig.getAttributes(), QA, null);

        // get initialize items
        // get record level items
        if (sQAName != null) {
            addQAForStep(sQAName);
        }

    }

    final public boolean addQAForItem(ETLPort eiItem, Node xmlNode) {
        Node QANode = getQAItemNode(XMLHelper.getAttributeAsString(xmlNode.getAttributes(), QA, null));

        if (QANode == null) {
            return false;
        }

        NodeList nl = QANode.getChildNodes();

        ArrayList qaItems = new ArrayList();

        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);

            if (n.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            String sQAClass = step.getQAClass(n.getNodeName());

            if (sQAClass == null) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Step does not support QA type of "
                        + n.getNodeName());
            }
            else {
                // Try to instantiate the step object...
                try {
                    Class cStepClass = Class.forName(sQAClass);
                    QAItemLevelEventGenerator qaObject = (QAItemLevelEventGenerator) cStepClass.newInstance();

                    qaObject.initialize(this.step, eiItem, n);

                    if (qaObject != null) {
                        if (this.aItemsToCheck == null) {
                            this.aItemsToCheck = new QAItemLevelEventGenerator[1];
                            this.aItemsToCheck[0] = (QAItemLevelEventGenerator) qaObject;
                        }
                        else {
                            QAItemLevelEventGenerator[] tmp = new QAItemLevelEventGenerator[this.aItemsToCheck.length + 1];
                            System.arraycopy(this.aItemsToCheck, 0, tmp, 0, this.aItemsToCheck.length);
                            this.aItemsToCheck = tmp;

                            this.aItemsToCheck[this.aItemsToCheck.length - 1] = (QAItemLevelEventGenerator) qaObject;
                        }
                        qaItems.add(qaObject);
                    }
                } catch (Exception e) {
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "QA collection could not create class "
                            + sQAClass);
                }
            }
        }

        if (qaItems.size() > 0) {
            QAItemLevelEventGenerator[] tmp = new QAItemLevelEventGenerator[qaItems.size()];

            qaItems.toArray(tmp);
            eiItem.setQAEventGenerators(tmp);

            return true;
        }

        return false;
    }

    final protected void addQAForStep(String QAName) {
        Node QANode = getQAItemNode(QAName);

        if (QANode == null) {
            return;
        }

        // should QA be ignored
        boolean bIgnore = XMLHelper.getAttributeAsBoolean(QANode.getAttributes(), "IGNORE", false);

        if (bIgnore == true) {
            return;
        }

        NodeList nl = QANode.getChildNodes();

        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);

            if (n.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            if (XMLHelper.getAttributeAsBoolean(n.getAttributes(), "IGNORE", false) == true) {
                continue;
            }

            String sQAClass = step.getQAClass(n.getNodeName());

            if (sQAClass == null) {
                ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Step does not support QA type of "
                        + n.getNodeName());
            }
            else {
                // Try to instantiate the step object...
                try {
                    Class cStepClass = Class.forName(sQAClass);
                    QAEventGenerator qaObject = (QAEventGenerator) cStepClass.newInstance();

                    qaObject.initialize(this.step, n);

                    if (qaObject != null) {
                        if (qaObject instanceof QAInitializeLevelEventGenerator) {

                            if (this.initializeLevel == null) {
                                this.initializeLevel = new QAInitializeLevelEventGenerator[1];
                                this.initializeLevel[0] = (QAInitializeLevelEventGenerator) qaObject;
                            }
                            else {
                                QAInitializeLevelEventGenerator[] tmp = new QAInitializeLevelEventGenerator[this.initializeLevel.length + 1];
                                System.arraycopy(this.initializeLevel, 0, tmp, 0, this.initializeLevel.length);
                                this.initializeLevel = tmp;

                                this.initializeLevel[this.initializeLevel.length - 1] = (QAInitializeLevelEventGenerator) qaObject;
                            }

                        }
                        else if (qaObject instanceof QARecordLevelEventGenerator) {

                            if (this.recordLevel == null) {
                                this.recordLevel = new QARecordLevelEventGenerator[1];
                                this.recordLevel[0] = (QARecordLevelEventGenerator) qaObject;
                            }
                            else {
                                QARecordLevelEventGenerator[] tmp = new QARecordLevelEventGenerator[this.recordLevel.length + 1];
                                System.arraycopy(this.recordLevel, 0, tmp, 0, this.recordLevel.length);
                                this.recordLevel = tmp;
                                this.recordLevel[this.recordLevel.length - 1] = (QARecordLevelEventGenerator) qaObject;
                            }
                        }
                        else
                            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE,
                                    "QA check level not supported, for class " + sQAClass);

                    }
                } catch (Exception e) {
                    ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "QA collection could not create class "
                            + sQAClass + ", tag most likely not supported by step");
                }
            }
        }
    }

    private Node getQAItemNode(String QAName) {
        if (QAName == null) {
            return null;
        }

        // get QA nodes <ETL><QA></QA></ETL>
        Node[] aQANodes = XMLHelper.findElementsByName(this.nQADefinition, QA, ETLStep.NAME_ATTRIB, QAName);

        if (aQANodes == null) {
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Could not locate QA item " + QAName);

            return null;
        }
        else if (aQANodes.length > 1) {
            ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE,
                    "First QA item will be chosen as multiple QA definitions exists for QA " + QAName);
        }

        return aQANodes[0];
    }

    public void completeCheck() throws KETLQAException {
        Metadata md = ResourcePool.getMetadata();

        if (recordLevel != null) {
            for (int i = recordLevel.length - 1; i >= 0; i--) {
                ETLEvent res = recordLevel[i].completeCheck();
                if (res != null)
                    recordLevel[i].fireEvent(res);
                if (md != null) {
                    recordHistory(md, recordLevel[i]);
                }
            }
        }

        if (this.aItemsToCheck != null) {
            for (int i = aItemsToCheck.length - 1; i >= 0; i--) {
                ETLEvent res = aItemsToCheck[i].completeCheck();
                if (res != null)
                    aItemsToCheck[i].fireEvent(res);
                if (md != null) {
                    recordHistory(md, aItemsToCheck[i]);
                }
            }
        }

        if (initializeLevel != null) {
            for (int i = initializeLevel.length - 1; i >= 0; i--) {

                if (md != null) {
                    recordHistory(md, initializeLevel[i]);
                }
            }
        }
    }

    public void initializeCheck() throws KETLThreadException {
        if (initializeLevel == null) {
            return;
        }

        for (int i = initializeLevel.length - 1; i >= 0; i--) {
            ETLEvent res = initializeLevel[i].InitializeCheck();
            if (res != null)
                initializeLevel[i].fireEvent(res);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#postPutNextRecordCheck()
     */
    public void recordCheck(Object[] rr, Exception e) throws KETLQAException {
        if (recordLevel == null) {
            return;
        }

        for (int i = recordLevel.length - 1; i >= 0; i--) {
            ETLEvent res = recordLevel[i].recordCheck(rr, e);
            if (res != null)
                recordLevel[i].fireEvent(res);
        }
    }

    final boolean recordHistory(Metadata md, QAEventGenerator qa) {
        if ((qa.getXMLHistory() != null) && qa.recordHistory()) {
            return md.recordQAHistory(step.getJobExecutor().getCurrentETLJob().getJobID(), this.step.toString(),
                    qa.mstrQAName, qa.getQAType(), step.getJobExecutor().getCurrentETLJob().getCreationDate(), qa
                            .getXMLHistory());
        }

        return true;
    }

    public String toString() {
        return ("QA Collection for step " + this.step);
    }

    public void itemChecks(Object[] di, Exception e) throws KETLQAException {
        if (this.aItemsToCheck != null) {
            for (int i = aItemsToCheck.length - 1; i >= 0; i--) {
                ETLEvent res = aItemsToCheck[i].itemCheck(di, e);
                if (res != null)
                    aItemsToCheck[i].fireEvent(res);

            }
        }

    }
}
