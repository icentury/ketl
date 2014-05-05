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
 * Created on Jul 5, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.sessionizer;

import com.kni.etl.stringtools.BoyerMooreAlgorithm;

// TODO: Auto-generated Javadoc
/**
 * The Class WebServerSettings.
 * 
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 * Generation&gt;Code and Comments
 */
public class WebServerSettings {

    /** The ch end markers. */
    char[][][] mChEndMarkers;
    
    /** The ch variable seperators. */
    char[][][] mChVariableSeperators;
    
    /** The st end markers. */
    String[][] mStEndMarkers;
    
    /** The st variable seperators. */
    String[][] mStVariableSeperators;
    
    /** The mb end markers. */
    BoyerMooreAlgorithm[][] mbEndMarkers;
    
    /** The mb variable seperators. */
    BoyerMooreAlgorithm[][] mbVariableSeperators;
    
    /** The Constant COOKIE. */
    public static final int COOKIE = 0;
    
    /** The Constant URL. */
    public static final int URL = 1;
    
    /** The Constant OTHER. */
    public static final int OTHER = 2;

    /**
     * Instantiates a new web server settings.
     */
    public WebServerSettings() {
        super();
        this.mChEndMarkers = new char[3][][];
        this.mChVariableSeperators = new char[3][][];
        this.mStEndMarkers = new String[3][];
        this.mStVariableSeperators = new String[3][];
        this.mbEndMarkers = new BoyerMooreAlgorithm[3][];
        this.mbVariableSeperators = new BoyerMooreAlgorithm[3][];
    }

    /**
     * Adds the web server pair.
     * 
     * @param pType the type
     * @param pEndMarkers the end markers
     * @param pVariableSeperators the variable seperators
     */
    void addWebServerPair(int pType, String[] pEndMarkers, String[] pVariableSeperators) {
        this.mChEndMarkers[pType] = new char[pEndMarkers.length][];
        this.mbEndMarkers[pType] = new BoyerMooreAlgorithm[pEndMarkers.length];
        this.mStEndMarkers[pType] = new String[pEndMarkers.length];

        for (int i = 0; i < pEndMarkers.length; i++) {
            BoyerMooreAlgorithm searchAccelerator = new BoyerMooreAlgorithm();
            searchAccelerator.compile(pEndMarkers[i]);
            this.mbEndMarkers[pType][i] = searchAccelerator;
            this.mChEndMarkers[pType][i] = pEndMarkers[i].toCharArray();
            this.mStEndMarkers[pType][i] = pEndMarkers[i];
        }

        this.mChVariableSeperators[pType] = new char[pVariableSeperators.length][];
        this.mbVariableSeperators[pType] = new BoyerMooreAlgorithm[pVariableSeperators.length];
        this.mStVariableSeperators[pType] = new String[pVariableSeperators.length];

        for (int i = 0; i < pVariableSeperators.length; i++) {
            BoyerMooreAlgorithm searchAccelerator = new BoyerMooreAlgorithm();
            searchAccelerator.compile(pVariableSeperators[i]);
            this.mbVariableSeperators[pType][i] = searchAccelerator;
            this.mChVariableSeperators[pType][i] = pVariableSeperators[i].toCharArray();
            this.mStVariableSeperators[pType][i] = pVariableSeperators[i];
        }
    }

    /**
     * Gets the end markers as string.
     * 
     * @param pType the type
     * 
     * @return the end markers as string
     */
    public String[] getEndMarkersAsString(int pType) {
        return this.mStEndMarkers[pType];
    }

    /**
     * Gets the end markers as boyer moore.
     * 
     * @param pType the type
     * 
     * @return the end markers as boyer moore
     */
    public BoyerMooreAlgorithm[] getEndMarkersAsBoyerMoore(int pType) {
        return this.mbEndMarkers[pType];
    }

    /**
     * Gets the end markers as char array.
     * 
     * @param pType the type
     * 
     * @return the end markers as char array
     */
    public char[][] getEndMarkersAsCharArray(int pType) {
        return this.mChEndMarkers[pType];
    }

    /**
     * Gets the seperators as boyer moore.
     * 
     * @param pType the type
     * 
     * @return the seperators as boyer moore
     */
    public BoyerMooreAlgorithm[] getSeperatorsAsBoyerMoore(int pType) {
        return this.mbVariableSeperators[pType];
    }

    /**
     * Gets the seperators as string.
     * 
     * @param pType the type
     * 
     * @return the seperators as string
     */
    public String[] getSeperatorsAsString(int pType) {
        return this.mStVariableSeperators[pType];
    }

    /**
     * Gets the seperators as char array.
     * 
     * @param pType the type
     * 
     * @return the seperators as char array
     */
    public char[][] getSeperatorsAsCharArray(int pType) {
        return this.mChVariableSeperators[pType];
    }
}
