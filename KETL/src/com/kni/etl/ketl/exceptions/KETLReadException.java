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
public class KETLReadException extends Exception {

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
    public KETLReadException() {
        super();
    }

    /**
     * @param message
     */
    public KETLReadException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public KETLReadException(String message, Throwable cause) {
        super(message,cause);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * @param cause
     */
    public KETLReadException(Throwable cause) {
        super(cause.getMessage(),cause);
        this.setStackTrace(cause.getStackTrace());
    }

    int code = 0;

    /**
     * @param pMessage
     */
    public KETLReadException(String pMessage, int pCode) {
        this(pMessage);
        code = pCode;
    }
}
