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
package com.kni.etl;

// TODO: Auto-generated Javadoc
/**
 * The Class PassphraseException.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class PassphraseException extends Exception {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    
    /** The m passphrase file path. */
    private String mPassphrase, mPassphraseFilePath;

    /**
     * The Constructor.
     * 
     * @param pMessage the message
     * @param pPassphrasePath TODO
     * @param pPassphrase the passphrase
     */
    public PassphraseException(String pMessage, String pPassphrase, String pPassphrasePath) {
        super(pMessage);
        this.mPassphrase = pPassphrase;
        this.mPassphraseFilePath = pPassphrasePath;
    }

    /**
     * Gets the passphrase used.
     * 
     * @return the passphrase used
     */
    public final String getPassphraseUsed() {
        return this.mPassphrase;
    }

    /**
     * Gets the passphrase file path.
     * 
     * @return the passphrase file path
     */
    public final String getPassphraseFilePath() {
        return this.mPassphraseFilePath;
    }
}
