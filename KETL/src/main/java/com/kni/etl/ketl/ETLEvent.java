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
 * Created on Jul 14, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl;

import com.kni.etl.ketl.qa.QAEventGenerator;


// TODO: Auto-generated Javadoc
/**
 * The Class ETLEvent.
 * 
 * @author bsullivan
 * 
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class ETLEvent
{
    
    /** The mstr event name. */
    public String mstrEventName;
    
    /** The mstr message. */
    String mstrMessage;
    
    /** The mo source. */
    Object moSource = null;
    
    /** The mes originating step. */
    RecordChecker mesOriginatingStep;
    
    /** The mi return code. */
    int miReturnCode = 0;

    /**
     * Instantiates a new ETL event.
     */
    public ETLEvent()
    {
        super();
    }

    /**
     * Instantiates a new ETL event.
     * 
     * @param esOriginatingStep the es originating step
     * @param strEventName the str event name
     */
    public ETLEvent(RecordChecker esOriginatingStep, String strEventName)
    {
        this();
        this.mstrEventName = strEventName;
        this.mesOriginatingStep = esOriginatingStep;
    }

    /**
     * Instantiates a new ETL event.
     * 
     * @param oSource the o source
     * @param esOriginatingStep the es originating step
     * @param strEventName the str event name
     * @param strMsg the str msg
     */
    public ETLEvent(Object oSource, RecordChecker esOriginatingStep, String strEventName, String strMsg)
    {
        this();
        this.moSource = oSource;
        this.mstrEventName = strEventName;
        this.mstrMessage = strMsg;
        this.mesOriginatingStep = esOriginatingStep;
    }

    /**
     * Gets the event name.
     * 
     * @return the event name
     */
    public String getEventName()
    {
        return this.mstrEventName;
    }

    /**
     * Sets the return code.
     * 
     * @param pReturnCode the new return code
     */
    public void setReturnCode(int pReturnCode)
    {
        this.miReturnCode = pReturnCode;
    }

    /**
     * Gets the return code.
     * 
     * @return the return code
     */
    public int getReturnCode()
    {
        return this.miReturnCode;
    }

    /**
     * Gets the ETL step.
     * 
     * @return the ETL step
     */
    public RecordChecker getETLStep()
    {
        return this.mesOriginatingStep;
    }

    /**
     * Gets the source object.
     * 
     * @return the source object
     */
    public Object getSourceObject()
    {
        return this.moSource;
    }

    /**
     * Gets the extended message.
     * 
     * @return the extended message
     */
    public String getExtendedMessage()
    {
        String extendedMessage = null;

        if ((this.moSource != null) && this.moSource instanceof QAEventGenerator)
        {
            extendedMessage = ((QAEventGenerator) this.moSource).getXMLHistory();
        }

        return extendedMessage;
    }
}
