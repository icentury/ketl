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

import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;

// TODO: Auto-generated Javadoc
/**
 * The Class KETLThreadException.
 * 
 * @author Owner To change the template for this generated type comment go to Window>Preferences>Java>Code
 * Generation>Code and Comments
 */
public class KETLThreadException extends Exception {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 4048797879017486642L;

    /**
     * Instantiates a new KETL thread exception.
     */
    public KETLThreadException() {
        super();
    }

    /** The source thread. */
    Thread sourceThread = Thread.currentThread();
    
    /** The source object. */
    private Object sourceObject;

    /**
     * Gets the source thread.
     * 
     * @return the source thread
     */
    public Thread getSourceThread() {
        return this.sourceThread;
    }

    /**
     * Gets the source object.
     * 
     * @return the source object
     */
    public Object getSourceObject() {
        return this.sourceObject;
    }

    /**
     * The Constructor.
     * 
     * @param message the message
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
     * The Constructor.
     * 
     * @param message the message
     * @param cause the cause
     * @param source TODO
     */
    public KETLThreadException(String message, Throwable cause, Object source) {
        super(message,cause);
        this.setSourceObject(source);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * The Constructor.
     * 
     * @param cause the cause
     * @param source TODO
     */
    public KETLThreadException(Throwable cause, Object source) {
        super(cause.getMessage(),cause);
        this.setSourceObject(source);
        this.setStackTrace(cause.getStackTrace());
    }

    /**
     * Sets the source object.
     * 
     * @param sourceObject the new source object
     */
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
