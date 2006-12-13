/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jun 2, 2006
 * 
 */
package com.kni.etl;


/**
 * @author nwakefield
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class PassphraseException extends Exception {

    private static final long serialVersionUID = 1L;
    private String mPassphrase,mPassphraseFilePath;
    /**
     * @param pMessage
     * @param pPassphrasePath TODO
     */
    public PassphraseException(String pMessage,String pPassphrase, String pPassphrasePath) {
        super(pMessage);
        this.mPassphrase = pPassphrase;
        this.mPassphraseFilePath = pPassphrasePath;
    }
    
    /**
     * @param pPassphrase The passphrase to return.
     */
    public final String getPassphraseUsed() {
        return this.mPassphrase;
    }
    
    public final String getPassphraseFilePath() {
        return this.mPassphraseFilePath;
    }
}
