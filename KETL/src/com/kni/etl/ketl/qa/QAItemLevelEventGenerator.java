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

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLEvent;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl_v1.DataItem;

/**
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public abstract class QAItemLevelEventGenerator extends QAEventGenerator {

    protected ETLPort mAssociatetPort = null;

    /**
     * @param eStep
     * @param nXMLConfig
     */
    public QAItemLevelEventGenerator() {
        super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#postInitializeCheck()
     */
    public final void postInitializeCheck() {
        // override so function does nothing
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#prePutNextRecordCheck()
     */
    public final void prePutNextRecordCheck(Object[] rr) {
        // override so function does nothing
    }

    final ETLEvent InitializeCheck() {
        // override so function does nothing
        return null;
    }

    final ETLEvent putNextRecordCheck(Object[] rr) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getCheckLevel()
     */
    final int getCheckLevel() {
        return ITEM;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#postGetItemCheck(int)
     */
    final public void postGetItemCheck(DataItem di) {
        super.postGetItemCheck(di);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#prePutItemCheck(int)
     */
    final public void prePutItemCheck(DataItem di) {
        super.prePutItemCheck(di);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#completeCheck()
     */
    ETLEvent completeCheck() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getItemCheck(com.kni.etl.DataItem)
     */
    ETLEvent getItemCheck(DataItem pDi) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getParameters()
     */
    void getParameters() {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getXMLHistory()
     */
    public String getXMLHistory() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#initialize(com.kni.etl.ketl.ETLStep, org.w3c.dom.Node)
     */
    public void initialize(ETLStep pStep, ETLPort pPort, Node pConfig) throws KETLThreadException {
        this.mAssociatetPort = pPort;

        super.initialize(pStep, pConfig);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#putItemCheck(com.kni.etl.DataItem)
     */
    ETLEvent putItemCheck(DataItem pDi) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#setQAName()
     */
    protected String setQAName() throws KETLThreadException {
        super.setQAName();

        if (this.mAssociatetPort != null)
            this.mstrQAName = this.mstrQAName + "-" + this.mAssociatetPort.getPortName();
        return this.mstrHistoryXML;
    }
}
