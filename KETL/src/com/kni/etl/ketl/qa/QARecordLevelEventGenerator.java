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
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.reader.ETLReader;
import com.kni.etl.ketl.transformation.ETLTransformation;
import com.kni.etl.ketl.writer.ETLWriter;

/**
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public abstract class QARecordLevelEventGenerator extends QAEventGenerator {

    public static final int SUCCESS = 0;
    public static final int ERROR = -1;

    public QARecordLevelEventGenerator() {
        super();
    }

    abstract ETLEvent completeCheck();

    @Override
    public void initialize(ETLStep eStep, Node nXMLConfig) throws KETLThreadException {

        if (!(eStep instanceof ETLReader || eStep instanceof ETLWriter || eStep instanceof ETLTransformation))
            throw new KETLThreadException("QA test does not support class " + this.getClass().getCanonicalName(), eStep);

        super.initialize(eStep, nXMLConfig);

    }

    abstract ETLEvent recordCheck(Object[] rr, Exception e);

}
