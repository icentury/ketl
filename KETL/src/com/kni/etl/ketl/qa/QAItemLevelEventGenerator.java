/*
 *  Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved. 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 *  
 *  Kinetic Networks Inc
 *  33 New Montgomery, Suite 1200
 *  San Francisco CA 94105
 *  http://www.kineticnetworks.com
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

// TODO: Auto-generated Javadoc
/**
 * The Class QAItemLevelEventGenerator.
 * 
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public abstract class QAItemLevelEventGenerator extends QAEventGenerator {

    /** The associatet port. */
    protected ETLPort mAssociatetPort = null;

    /**
     * The Constructor.
     */
    public QAItemLevelEventGenerator() {
        super();
    }

    /**
     * Item check.
     * 
     * @param di the di
     * @param exception the exception
     * 
     * @return the ETL event
     */
    final ETLEvent itemCheck(Object[] di, Exception exception) {
        return this.itemCheck(di[this.mAssociatetPort.getPortIndex()], exception, this.mAssociatetPort.getPortClass());
    }

    /**
     * Item check.
     * 
     * @param di the di
     * @param exception the exception
     * @param expectedClass the expected class
     * 
     * @return the ETL event
     */
    abstract ETLEvent itemCheck(Object di, Exception exception, Class expectedClass);

    /**
     * Initialize.
     * 
     * @param eStep the e step
     * @param pPort the port
     * @param pConfig the config
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public void initialize(ETLStep eStep, ETLPort pPort, Node pConfig) throws KETLThreadException {

        if (!(eStep instanceof ETLReader || eStep instanceof ETLWriter || eStep instanceof ETLTransformation))
            throw new KETLThreadException("QA test does not support class " + this.getClass().getCanonicalName(), eStep);

        this.mAssociatetPort = pPort;
        super.initialize(eStep, pConfig);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.qa.QAEventGenerator#setQAName()
     */
    @Override
    protected String setQAName() throws KETLThreadException {
        super.setQAName();

        if (this.mAssociatetPort != null)
            this.mstrQAName = this.mstrQAName + "-" + this.mAssociatetPort.getPortName();
        return this.mstrHistoryXML;
    }

    /**
     * Complete check.
     * 
     * @return the ETL event
     */
    abstract ETLEvent completeCheck();
}
