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
 * Created on Jul 30, 2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.ketl.exceptions;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLReadException.
 * 
 * @author Owner To change the template for this generated type comment go to Window>Preferences>Java>Code
 * Generation>Code and Comments
 */
public class KETLReadException extends Exception {

    /** The source thread. */
    Thread sourceThread = Thread.currentThread();

    /**
     * Gets the source thread.
     * 
     * @return the source thread
     */
    public Thread getSourceThread() {
        return this.sourceThread;
    }

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 4048797879017486642L;

    /**
     * Instantiates a new KETL read exception.
     */
    public KETLReadException() {
        super();
    }

    /**
     * The Constructor.
     * 
     * @param message the message
     */
    public KETLReadException(String message) {
        super(message);
    }

    /**
     * The Constructor.
     * 
     * @param message the message
     * @param cause the cause
     */
    public KETLReadException(String message, Throwable cause) {
        super(message,cause);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * The Constructor.
     * 
     * @param cause the cause
     */
    public KETLReadException(Throwable cause) {
        super(cause.getMessage(),cause);
        this.setStackTrace(cause.getStackTrace());
    }

    /** The code. */
    int code = 0;

    /**
     * The Constructor.
     * 
     * @param pMessage the message
     * @param pCode the code
     */
    public KETLReadException(String pMessage, int pCode) {
        this(pMessage);
        this.code = pCode;
    }
}
