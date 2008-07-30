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
package com.kni.etl.ketl;

import org.w3c.dom.Node;

import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (3/5/2002 3:41:34 PM)
 * 
 * @author: Administrator
 */
public class ETLTrigger {

    /** The mes target step. */
    private ETLStep mesTargetStep;
    
    /** The mstr event. */
    private String mstrEvent;
    
    /** The mstr handler. */
    private String mstrHandler;
    
    /** The mstr target step. */
    private String mstrTargetStep;
    
    /** The mes step. */
    private ETLStep mesStep;

    /**
     * Instantiates a new ETL trigger.
     */
    public ETLTrigger() {
        super();
    }

    /**
     * Instantiates a new ETL trigger.
     * 
     * @param esOwningStep the es owning step
     */
    public ETLTrigger(ETLStep esOwningStep) {
        this();
        this.mesStep = esOwningStep;
    }

    /**
     * Supports.
     * 
     * @param event the event
     * 
     * @return true, if successful
     */
    public boolean supports(ETLEvent event) {
        return this.mstrEvent.equals(event.mstrEventName);
    }

    /**
     * Execute.
     * 
     * @param event the event
     * 
     * @return the int
     * 
     * @throws KETLQAException the KETLQA exception
     */
    public int execute(ETLEvent event) throws KETLQAException {
        return this.mesTargetStep.handleEvent(this.mstrHandler, event);
    }

    /**
     * Gets the event.
     * 
     * @return the event
     */
    public String getEvent() {
        return this.mstrEvent;
    }

    // Initializes the basic attributes and checks the child nodes to relate to other steps via the HashMap.
    /**
     * Initialize.
     * 
     * @param xmlConfig the xml config
     * @param step the step
     * 
     * @return the int
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public int initialize(Node xmlConfig, ETLStep step) throws KETLThreadException {

        // Pull the name of the event...
        this.mstrEvent = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "EVENT", null);
        // Pull the name of the step to be called...
        this.mstrTargetStep = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "STEP", null);
        this.mstrHandler = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "HANDLER", null);

        // Now look in the hash map for the step and point to it's object...
        if (this.mstrTargetStep == null)
            this.mesTargetStep = this.mesStep;
        else
            this.mesTargetStep = step.getTargetStep(this.mstrTargetStep);

        return 0;
    }
}
