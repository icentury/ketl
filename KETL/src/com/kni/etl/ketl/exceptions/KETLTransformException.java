/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 12, 2006
 * 
 */
package com.kni.etl.ketl.exceptions;



/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class KETLTransformException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 4580348018625585073L;
    int code = 0;

    /**
     * @param pMessage
     */
    public KETLTransformException(String pMessage, int pCode) {
        super(pMessage);
        code = pCode;
    }
    
    /**
     * @param pMessage
     */
    public KETLTransformException(String pMessage, Exception e) {
        super(pMessage,e);        
        this.setStackTrace(e.getStackTrace());
    }

    public KETLTransformException(Exception e) {        
        super(e.getMessage(),e);
        this.setStackTrace(e.getStackTrace());
    }
    
    public KETLTransformException(Error e) {        
        super(e.getMessage(),e);
        this.setStackTrace(e.getStackTrace());
    }

    public KETLTransformException(String string) {
        super(string);
    }


    Thread sourceThread = Thread.currentThread();

    public Thread getSourceThread() {
        return sourceThread;
    }


}
