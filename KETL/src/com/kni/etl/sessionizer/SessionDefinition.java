/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;

/**
 * Insert the type's description here. Creation date: (4/8/2002 4:22:35 PM)
 * 
 * @author: Administrator
 */
public class SessionDefinition {

    public SessionIdentifier[] SessionIdentifiers;
    public int TimeOut = 0;
    public int IPBrowserTimeOut;
    public int MainIdentifierTimeOut = 0;
    public int FirstClickIdentifierTimeOut = 0;
    public int PersistantIdentifierTimeOut = 0;
    public int WebServerType = -1;
    public int PeakSessionsAnHour = 5000;
    public boolean IPBrowserFallbackEnabled = false;
    public boolean MainIdentifierFallbackEnabled = false;
    public boolean FirstClickIdentifierFallbackEnabled = false;
    public boolean PersistantIdentifierFallbackEnabled = false;
    public boolean IPBrowserExpireWhenBetterMatch = false;
    public boolean MainIdentifierExpireWhenBetterMatch = false;
    public boolean FirstClickIdentifierExpireWhenBetterMatch = false;
    public boolean PersistantIdentifierExpireWhenBetterMatch = false;
    protected int NumberOfIdentifiers = 0;

    /**
     * SessionDefinition constructor comment.
     */
    public SessionDefinition() {
        super();
    }

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
