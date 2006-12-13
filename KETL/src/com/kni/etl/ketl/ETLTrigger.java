/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl;

import java.util.HashMap;

import org.w3c.dom.Node;

import com.kni.etl.util.XMLHelper;


/**
 * Insert the type's description here.
 * Creation date: (3/5/2002 3:41:34 PM)
 * @author: Administrator
 */
public class ETLTrigger 
{
    private ETLStep mesTargetStep;
    private String mstrEvent;
    private String mstrHandler;
    private String mstrTargetStep;
    private ETLStep mesStep;

    public ETLTrigger()
    {
        super();
    }

    public ETLTrigger(ETLStep esOwningStep)
    {
        this();
        mesStep = esOwningStep;
    }

    public boolean supports(ETLEvent event)
    {
        return mstrEvent.equals(event.mstrEventName);
    }

    public int execute(ETLEvent event)
    {
        return mesTargetStep.handleEvent(mstrHandler, event);
    }

    public String getEvent()
    {
        return mstrEvent;
    }

    // Initializes the basic attributes and checks the child nodes to relate to other steps via the HashMap.
    public int initialize(Node xmlConfig, HashMap hmSteps, String strStep)
    {


        // Pull the name of the event...
        try
        {
            mstrEvent = xmlConfig.getAttributes().getNamedItem("EVENT").getNodeValue();
        }
        catch (Exception e)
        {
            // REVIEW: Should we do anything here?
        }

        // Pull the name of the step to be called...
        try
        {
            mstrTargetStep = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "STEP", null);
        }
        catch (Exception e)
        {
            // REVIEW: Should we do anything here?
        }

        // Pull the name of the method to be called...
        try
        {
            mstrHandler = xmlConfig.getAttributes().getNamedItem("HANDLER").getNodeValue();
        }
        catch (Exception e)
        {
            // REVIEW: Should we do anything here?
        }

        // Now look in the hash map for the step and point to it's object...
        if (mstrTargetStep == null)
        {
            mesTargetStep = mesStep;
        }
        else if ((mesTargetStep = (ETLStep) hmSteps.get(mstrTargetStep)) == null)
        {
            return -2;
        }

        return 0;
    }
}
