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
import com.kni.etl.ketl.exceptions.KETLThreadException;

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

    abstract ETLEvent InitializeCheck() throws KETLThreadException;

}
