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

import com.kni.etl.ketl.ETLEvent;
import com.kni.etl.ketl_v1.DataItem;

/**
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public abstract class QAInitializeLevelEventGenerator extends QAEventGenerator {

    /**
     * @param eStep
     * @param nXMLConfig
     */
    public QAInitializeLevelEventGenerator() {
        super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#prePutNextRecordCheck()
     */
    public final void prePutNextRecordCheck(Object[] rr) {
        // override so function does nothing
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
        return INITIALIZE;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#postGetItemCheck(int)
     */
    final public void postGetItemCheck(int iItemPos) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#prePutItemCheck(int)
     */
    final public void prePutItemCheck(int iItemPos) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getItemCheck(int)
     */
    final String getItemCheck(int iItemPos) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#putItemCheck(int)
     */
    final String putItemCheck(int iItemPos) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#completeCheck()
     */
    abstract ETLEvent completeCheck();

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getItemCheck(com.kni.etl.DataItem)
     */
    final ETLEvent getItemCheck(DataItem di) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getXMLHistory()
     */
    abstract public String getXMLHistory();

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#InitializeCheck()
     */
    abstract ETLEvent InitializeCheck();

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#postGetItemCheck(com.kni.etl.DataItem)
     */
    public void postGetItemCheck(DataItem di) {
        super.postGetItemCheck(di);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#prePutItemCheck(com.kni.etl.DataItem)
     */
    public void prePutItemCheck(DataItem di) {
        super.prePutItemCheck(di);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#putItemCheck(com.kni.etl.DataItem)
     */
    final ETLEvent putItemCheck(DataItem di) {
        throw new RuntimeException("");
    }
}
