/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

/**
 * @author nicholas.wakefield TODO To change the template for this generated type comment go to Window - Preferences -
 *         Java - Code Style - Code Templates
 */
public class KETLQAException extends KETLThreadException {

    /**
     *
     */
    private static final long serialVersionUID = 3761971553512075575L;
    ETLEvent etlEvent = null;
    ETLStep etlStep = null;

    /**
     * @param message
     */
    public KETLQAException(String message, ETLEvent etlEvent, ETLStep etlStep) {
        super(message,etlStep);
        this.etlEvent = etlEvent;
        this.etlStep = etlStep;
    }

    /**
     * @return Returns the etlStep.
     */
    public ETLStep getETLStep() {
        return etlStep;
    }

    public int getErrorCode() {
        return this.etlEvent.getReturnCode();
    }

    public ETLEvent getETLEvent() {
        return this.etlEvent;
    }
}
