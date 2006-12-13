/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 14, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl;

import com.kni.etl.ketl.qa.QAEventGenerator;


/**
 * @author bsullivan
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class ETLEvent
{
    public String mstrEventName;
    String mstrMessage;
    Object moSource = null;
    ETLStep mesOriginatingStep;
    int miReturnCode = 0;

    public ETLEvent()
    {
        super();
    }

    public ETLEvent(ETLStep esOriginatingStep, String strEventName)
    {
        this();
        mstrEventName = strEventName;
        mesOriginatingStep = esOriginatingStep;
    }

    public ETLEvent(Object oSource, ETLStep esOriginatingStep, String strEventName, String strMsg)
    {
        this();
        moSource = oSource;
        mstrEventName = strEventName;
        mstrMessage = strMsg;
        mesOriginatingStep = esOriginatingStep;
    }

    /**
     * @return
     */
    public String getEventName()
    {
        return mstrEventName;
    }

    public void setReturnCode(int pReturnCode)
    {
        miReturnCode = pReturnCode;
    }

    public int getReturnCode()
    {
        return miReturnCode;
    }

    public ETLStep getETLStep()
    {
        return this.mesOriginatingStep;
    }

    public Object getSourceObject()
    {
        return this.moSource;
    }

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
