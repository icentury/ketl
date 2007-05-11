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
package com.kni.etl.sessionizer;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (4/8/2002 4:22:35 PM)
 * 
 * @author: Administrator
 */
public class SessionDefinition {

    /** The Session identifiers. */
    public SessionIdentifier[] SessionIdentifiers;
    
    /** The Time out. */
    public int TimeOut = 0;
    
    /** The IP browser time out. */
    public int IPBrowserTimeOut;
    
    /** The Main identifier time out. */
    public int MainIdentifierTimeOut = 0;
    
    /** The First click identifier time out. */
    public int FirstClickIdentifierTimeOut = 0;
    
    /** The Persistant identifier time out. */
    public int PersistantIdentifierTimeOut = 0;
    
    /** The Web server type. */
    public int WebServerType = -1;
    
    /** The Peak sessions an hour. */
    public int PeakSessionsAnHour = 5000;
    
    /** The IP browser fallback enabled. */
    public boolean IPBrowserFallbackEnabled = false;
    
    /** The Main identifier fallback enabled. */
    public boolean MainIdentifierFallbackEnabled = false;
    
    /** The First click identifier fallback enabled. */
    public boolean FirstClickIdentifierFallbackEnabled = false;
    
    /** The Persistant identifier fallback enabled. */
    public boolean PersistantIdentifierFallbackEnabled = false;
    
    /** The IP browser expire when better match. */
    public boolean IPBrowserExpireWhenBetterMatch = false;
    
    /** The Main identifier expire when better match. */
    public boolean MainIdentifierExpireWhenBetterMatch = false;
    
    /** The First click identifier expire when better match. */
    public boolean FirstClickIdentifierExpireWhenBetterMatch = false;
    
    /** The Persistant identifier expire when better match. */
    public boolean PersistantIdentifierExpireWhenBetterMatch = false;
    
    /** The Number of identifiers. */
    protected int NumberOfIdentifiers = 0;

    /**
     * SessionDefinition constructor comment.
     */
    public SessionDefinition() {
        super();
    }

    /**
     * Match in valid.
     * 
     * @param iLastAlgorithmMatchedOn The last algorithm matched on
     * @param iAlgorithmUsed The algorithm used
     * 
     * @return true, if successful
     */
    public boolean matchInValid(int iLastAlgorithmMatchedOn, int iAlgorithmUsed) {
        if (iLastAlgorithmMatchedOn == 0) {
            return false;
        }

        switch (iAlgorithmUsed) {
        case 1: // MainSessionIdentifier

            if (this.MainIdentifierExpireWhenBetterMatch && (iLastAlgorithmMatchedOn < iAlgorithmUsed)) {
                return true;
            }

            break;

        case 2: // FirstClickSessionIdentifier

            if (this.FirstClickIdentifierExpireWhenBetterMatch && (iLastAlgorithmMatchedOn < iAlgorithmUsed)) {
                return true;
            }

            break;

        case 4: // PersistantIdentifier

            if (this.PersistantIdentifierExpireWhenBetterMatch && (iLastAlgorithmMatchedOn < iAlgorithmUsed)) {
                return true;
            }

            break;

        case 24: // IPAddressAndBrowser

            if (this.IPBrowserExpireWhenBetterMatch && (iLastAlgorithmMatchedOn < iAlgorithmUsed)) {
                return true;
            }

            break;
        }

        return false;
    }

    /**
     * Insert the method's description here. Creation date: (4/9/2002 2:39:09 PM)
     * 
     * @param pSessionIdentifier datasources.SessionIdentifier
     */
    public void addSessionIdentifier(SessionIdentifier pSessionIdentifier) {
        if (this.SessionIdentifiers == null) {
            this.SessionIdentifiers = new SessionIdentifier[this.NumberOfIdentifiers + 1];
        }
        else {
            SessionIdentifier[] tmp = new SessionIdentifier[this.NumberOfIdentifiers + 1];
            System.arraycopy(this.SessionIdentifiers, 0, tmp, 0, this.SessionIdentifiers.length);
            this.SessionIdentifiers = tmp;
        }

        // insert pSessionIdentifier at end of array
        this.SessionIdentifiers[this.NumberOfIdentifiers] = pSessionIdentifier;
        this.NumberOfIdentifiers++;
    }
}
