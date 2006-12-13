/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 30, 2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.exceptions;

import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;

/**
 * @author Owner To change the template for this generated type comment go to Window>Preferences>Java>Code
 *         Generation>Code and Comments
 */
public class KETLThreadException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = 4048797879017486642L;

    /**
     *
     */
    public KETLThreadException() {
        super();
    }

    Thread sourceThread = Thread.currentThread();
    private Object sourceObject;

    public Thread getSourceThread() {
        return sourceThread;
    }

    public Object getSourceObject() {
        return this.sourceObject;
    }

    /**
     * @param message
     * @param source TODO
     */
    public KETLThreadException(String message, Object source) {
        super(message);

        if (source instanceof Throwable) {
            this.setStackTrace(((Throwable) source).getStackTrace());
            this.setSourceObject(Thread.currentThread());
        }
        else
            this.setSourceObject(source);
    }

    /**
     * @param message
     * @param cause
     * @param source TODO
     */
    public KETLThreadException(String message, Throwable cause, Object source) {
        super(message,cause);
        this.setSourceObject(source);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * @param cause
     * @param source TODO
     */
    public KETLThreadException(Throwable cause, Object source) {
        super(cause.getMessage(),cause);
        this.setSourceObject(source);
        this.setStackTrace(cause.getStackTrace());
    }

    void setSourceObject(Object sourceObject) {
        this.sourceObject = sourceObject;
        if (this.sourceObject instanceof ETLPort) {
            ETLStep es = ((ETLPort) this.sourceObject).mesSrcStep;
            if (es != null) {
                es.logException(this);
            }
        }
        else if (this.sourceObject instanceof ETLStep) {
            ETLStep es = (ETLStep) this.sourceObject;
            es.logException(this);
        }
    }

}
