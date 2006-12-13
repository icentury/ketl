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

/**
 * @author Owner To change the template for this generated type comment go to Window>Preferences>Java>Code
 *         Generation>Code and Comments
 */
public class KETLWriteException extends Exception {

    Thread sourceThread = Thread.currentThread();

    public Thread getSourceThread() {
        return sourceThread;
    }

    /**
     *
     */
    private static final long serialVersionUID = 4048797879017486642L;

    /**
     *
     */
    public KETLWriteException() {
        super();
    }

    /**
     * @param message
     */
    public KETLWriteException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public KETLWriteException(String message, Throwable cause) {
        super(cause.getMessage(),cause);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * @param cause
     */
    public KETLWriteException(Throwable cause) {
        super(cause.getMessage(),cause);
        this.setStackTrace(cause.getStackTrace());
    }
}
