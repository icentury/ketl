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
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.reader.ETLReader;
import com.kni.etl.ketl.transformation.ETLTransformation;
import com.kni.etl.ketl.writer.ETLWriter;

// TODO: Auto-generated Javadoc
/**
 * The Class QARecordLevelEventGenerator.
 * 
 * @author nwakefield Creation Date: Jul 8, 2003
 */
public abstract class QARecordLevelEventGenerator extends QAEventGenerator {

    /** The Constant SUCCESS. */
    public static final int SUCCESS = 0;
    
    /** The Constant ERROR. */
    public static final int ERROR = -1;

    /**
     * Instantiates a new QA record level event generator.
     */
    public QARecordLevelEventGenerator() {
        super();
    }

    /**
     * Complete check.
     * 
     * @return the ETL event
     */
    abstract ETLEvent completeCheck();

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.qa.QAEventGenerator#initialize(com.kni.etl.ketl.ETLStep, org.w3c.dom.Node)
     */
    @Override
    public void initialize(ETLStep eStep, Node nXMLConfig) throws KETLThreadException {

        if (!(eStep instanceof ETLReader || eStep instanceof ETLWriter || eStep instanceof ETLTransformation))
            throw new KETLThreadException("QA test does not support class " + this.getClass().getCanonicalName(), eStep);

        super.initialize(eStep, nXMLConfig);

    }

    /**
     * Record check.
     * 
     * @param rr the rr
     * @param e the e
     * 
     * @return the ETL event
     */
    abstract ETLEvent recordCheck(Object[] rr, Exception e);

}
