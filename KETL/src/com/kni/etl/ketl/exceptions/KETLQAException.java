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
 * Created on Jul 12, 2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.kni.etl.ketl.exceptions;

import com.kni.etl.ketl.ETLEvent;
import com.kni.etl.ketl.ETLStep;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLQAException.
 * 
 * @author nicholas.wakefield TODO To change the template for this generated type comment go to Window - Preferences -
 * Java - Code Style - Code Templates
 */
public class KETLQAException extends KETLThreadException {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3761971553512075575L;
    
    /** The etl event. */
    ETLEvent etlEvent = null;
    
    /** The etl step. */
    ETLStep etlStep = null;

    /**
     * The Constructor.
     * 
     * @param message the message
     * @param etlEvent the etl event
     * @param etlStep the etl step
     */
    public KETLQAException(String message, ETLEvent etlEvent, ETLStep etlStep) {
        super(message,etlStep);
        this.etlEvent = etlEvent;
        this.etlStep = etlStep;
    }

    /**
     * Gets the ETL step.
     * 
     * @return Returns the etlStep.
     */
    public ETLStep getETLStep() {
        return this.etlStep;
    }

    /**
     * Gets the error code.
     * 
     * @return the error code
     */
    public int getErrorCode() {
        return this.etlEvent.getReturnCode();
    }

    /**
     * Gets the ETL event.
     * 
     * @return the ETL event
     */
    public ETLEvent getETLEvent() {
        return this.etlEvent;
    }
}
