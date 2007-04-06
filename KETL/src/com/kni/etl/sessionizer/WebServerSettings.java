/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

/*
 * Created on Jul 5, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.sessionizer;

import com.kni.etl.stringtools.BoyerMooreAlgorithm;

/**
 * @author nwakefield To change the template for this generated type comment go to Window&gt;Preferences&gt;Java&gt;Code
 *         Generation&gt;Code and Comments
 */
public class WebServerSettings {

    char[][][] mChEndMarkers;
    char[][][] mChVariableSeperators;
    String[][] mStEndMarkers;
    String[][] mStVariableSeperators;
    BoyerMooreAlgorithm[][] mbEndMarkers;
    BoyerMooreAlgorithm[][] mbVariableSeperators;
    public static final int COOKIE = 0;
    public static final int URL = 1;
    public static final int OTHER = 2;

    /**
     *
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

    public String[] getEndMarkersAsString(int pType) {
        return this.mStEndMarkers[pType];
    }

    public BoyerMooreAlgorithm[] getEndMarkersAsBoyerMoore(int pType) {
        return this.mbEndMarkers[pType];
    }

    public char[][] getEndMarkersAsCharArray(int pType) {
        return this.mChEndMarkers[pType];
    }

    public BoyerMooreAlgorithm[] getSeperatorsAsBoyerMoore(int pType) {
        return this.mbVariableSeperators[pType];
    }

    public String[] getSeperatorsAsString(int pType) {
        return this.mStVariableSeperators[pType];
    }

    public char[][] getSeperatorsAsCharArray(int pType) {
        return this.mChVariableSeperators[pType];
    }
}
