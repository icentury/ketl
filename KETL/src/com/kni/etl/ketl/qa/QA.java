/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 3, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.qa;

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;

/**
 * @author nwakefield Creation Date: Jul 3, 2003
 */
public abstract class QA {

    Node nQADefinition;
    ETLStep step = null;

    /**
     * @param eStep
     * @param nXMLConfig
     * @throws KETLThreadException 
     */
    public void initialize(ETLStep eStep, Node nXMLConfig) throws KETLThreadException {
        step = eStep;
        nQADefinition = nXMLConfig;
    }

    abstract public void postCompleteCheck();

    abstract public void postInitializeCheck();

    abstract public void prePutNextRecordCheck(Object[] rr);
}
