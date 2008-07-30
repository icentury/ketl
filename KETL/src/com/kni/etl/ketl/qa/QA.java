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
 * Created on Jul 3, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.qa;

import org.w3c.dom.Node;

import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;

// TODO: Auto-generated Javadoc
/**
 * The Class QA.
 * 
 * @author nwakefield Creation Date: Jul 3, 2003
 */
public abstract class QA {

    /** The n QA definition. */
    Node nQADefinition;
    
    /** The step. */
    ETLStep step = null;

    /**
     * Initialize.
     * 
     * @param eStep the e step
     * @param nXMLConfig the n XML config
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public void initialize(ETLStep eStep, Node nXMLConfig) throws KETLThreadException {
        this.step = eStep;
        this.nQADefinition = nXMLConfig;

    }

}
