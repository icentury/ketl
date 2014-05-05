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
package com.kni.etl.ketl.exceptions;



// TODO: Auto-generated Javadoc
/**
 * The Class KETLTransformException.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class KETLTransformException extends Exception {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 4580348018625585073L;
    
    /** The code. */
    int code = 0;

    /**
     * The Constructor.
     * 
     * @param pMessage the message
     * @param pCode the code
     */
    public KETLTransformException(String pMessage, int pCode) {
        super(pMessage);
        this.code = pCode;
    }
    
    /**
     * The Constructor.
     * 
     * @param pMessage the message
     * @param e the e
     */
    public KETLTransformException(String pMessage, Exception e) {
        super(pMessage,e);        
        this.setStackTrace(e.getStackTrace());
    }

    /**
     * Instantiates a new KETL transform exception.
     * 
     * @param e the e
     */
    public KETLTransformException(Exception e) {        
        super(e.getMessage(),e);
        this.setStackTrace(e.getStackTrace());
    }
    
    /**
     * Instantiates a new KETL transform exception.
     * 
     * @param e the e
     */
    public KETLTransformException(Error e) {        
        super(e.getMessage(),e);
        this.setStackTrace(e.getStackTrace());
    }

    /**
     * Instantiates a new KETL transform exception.
     * 
     * @param string the string
     */
    public KETLTransformException(String string) {
        super(string);
    }


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


}
