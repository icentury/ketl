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
import com.kni.etl.ketl.reader.ETLReader;
import com.kni.etl.ketl.transformation.ETLTransformation;
import com.kni.etl.ketl.writer.ETLWriter;

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

    final ETLEvent itemCheck(Object[] di, Exception exception) {
        return this.itemCheck(di[this.mAssociatetPort.getPortIndex()], exception, this.mAssociatetPort.getPortClass());
    }

    abstract ETLEvent itemCheck(Object di, Exception exception, Class expectedClass);

    public void initialize(ETLStep eStep, ETLPort pPort, Node pConfig) throws KETLThreadException {

        if (!(eStep instanceof ETLReader || eStep instanceof ETLWriter || eStep instanceof ETLTransformation))
            throw new KETLThreadException("QA test does not support class " + this.getClass().getCanonicalName(), eStep);

        this.mAssociatetPort = pPort;
        super.initialize(eStep, pConfig);
    }

    protected String setQAName() throws KETLThreadException {
        super.setQAName();

        if (this.mAssociatetPort != null)
            this.mstrQAName = this.mstrQAName + "-" + this.mAssociatetPort.getPortName();
        return this.mstrHistoryXML;
    }

    abstract ETLEvent completeCheck();
}
