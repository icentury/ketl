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

/**
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public abstract class QARecordLevelEventGenerator extends QAEventGenerator {

    /**
     * @param eStep
     * @param nXMLConfig
     */
    public QARecordLevelEventGenerator() {
        super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#postInitializeCheck()
     */
    public final void postInitializeCheck() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QA#prePutNextRecordCheck()
     */
    public final void prePutNextRecordCheck(Object[] rr) {
        super.prePutNextRecordCheck(rr);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#getCheckLevel()
     */
    final int getCheckLevel() {
        return RECORD;
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
    final ETLEvent getItemCheck(int iItemPos) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#InitializeCheck()
     */
    final ETLEvent InitializeCheck() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.kni.etl.ketl.qa.QAEventGenerator#putItemCheck(int)
     */
    final ETLEvent putItemCheck(int iItemPos) {
        return null;
    }
}
