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
 * Created on Jun 17, 2003
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package com.kni.etl.sessionizer;

import java.io.Serializable;
import java.util.List;

import com.kni.etl.dbutils.ResourcePool;

// TODO: Auto-generated Javadoc
/**
 * The Class AnalyzePageview.
 * 
 * @author nwakefield Creation Date: Jun 17, 2003
 */
public class AnalyzePageview implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The Constant RESTART_SESSIONS_NO_STORE. */
    public final static int RESTART_SESSIONS_NO_STORE = 1;
    
    /** The Constant DISABLE_RESTART_NO_STORE. */
    public final static int DISABLE_RESTART_NO_STORE = 2;
    
    /** The Constant DISABLE_RESTART_AND_STORE. */
    public final static int DISABLE_RESTART_AND_STORE = 3;
    
    /** The Constant RESTART_SESSIONS_AND_STORE. */
    public final static int RESTART_SESSIONS_AND_STORE = 4;

    /** The page view item accelerator. */
    transient PageViewItemFinderAccelerator pageViewItemAccelerator = null;
    
    /** The p def. */
    transient PageParserPageDefinition[] pDef = null;
    
    /** The session def. */
    transient SessionDefinition sessionDef = null;
    
    /** The close out mode. */
    int closeOutMode = AnalyzePageview.DISABLE_RESTART_AND_STORE;
    
    /** The mb pages only. */
    boolean mbPagesOnly = false;
    
    /** The s builder. */
    SessionBuilder sBuilder = null;

    /**
     * The Class Holder.
     */
    static public class Holder {

        /** The current session. */
        public Session currentSession;
        
        /** The page view. */
        public Object[] pageView;
        
        /** The is page view. */
        public boolean isPageView;
        
        /** The b cleansed. */
        public boolean bCleansed;
        
        /** The i cleansed ID. */
        public short iCleansedID;
        
        /** The i page sequence. */
        public short iPageSequence;
        
        /** The i associated hits. */
        public int iAssociatedHits;
    }

    /**
     * Analyze hit.
     * 
     * @param holder the holder
     * 
     * @return the holder
     * 
     * @throws Exception the exception
     */
    public Holder analyzeHit(Holder holder) throws Exception {
        Holder res = this.sBuilder.analyzeHit(holder);

        if (res == null && this.sBuilder.ignoreHit)
            return null;

        if (res == null || res.currentSession == null) {
            this.outputBadSession(holder.pageView);
            return null;
        }

        // get the page details
        if (res.currentSession != null) {
            // increments hits for session
            res.currentSession.Hit++;
            // keep reference to last page
            res.currentSession.lastHit = holder.pageView;

        }
        return holder;
    }

    /** The i bad sessions. */
    transient IDCounter iBadSessions = new IDCounter();

    /**
     * Output bad session.
     * 
     * @param resultRecord the result record
     */
    private void outputBadSession(Object[] resultRecord) {
        this.iBadSessions.incrementID();
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Invalid pagev view:"
                + java.util.Arrays.toString(resultRecord));
    }

    /**
     * Configure.
     * 
     * @param idCounter the id counter
     * @param sessionDef the session def
     * @param pDef the def
     * @param pPagesOnly the pages only
     * @param hitCountRequired the hit count required
     * @param itemMap the item map
     * @param completeSessionList the complete session list
     */
    public void configure(IDCounter idCounter, SessionDefinition sessionDef, PageParserPageDefinition[] pDef,
            boolean pPagesOnly, boolean hitCountRequired, int[] itemMap, List completeSessionList) {

        this.mbPagesOnly = pPagesOnly;
        this.sessionDef = sessionDef;
        this.pDef = pDef;

        // if this is a restart then reconfigure the session builder
        if (this.sBuilder == null) {
            this.sBuilder = new SessionBuilder(idCounter, itemMap, completeSessionList);
        }
        else {
            this.sBuilder.setIDCounter(idCounter);
            this.sBuilder.preLoadWebServerSettings();
        }

        this.sBuilder.setPagesOnly(pDef, this.mbPagesOnly, hitCountRequired);
        this.sBuilder.setSessionDefinition(sessionDef);

        if (this.pageViewItemAccelerator == null)
            this.pageViewItemAccelerator = new PageViewItemFinderAccelerator();

    }

    /**
     * Close.
     * 
     * @param fatalError the fatal error
     */
    public void close(boolean fatalError) {

        // mark remaining sessions done or still open
        if (fatalError == false) {
            if (this.closeOutMode == AnalyzePageview.DISABLE_RESTART_AND_STORE) {
                // store last hit date last activity
                this.sBuilder.closeOutAllSessions(false);
            }
            else if (this.closeOutMode == AnalyzePageview.RESTART_SESSIONS_AND_STORE) { // store null for last activity,
                                                                                        // makes
                // identification of non closed
                // sessions easier
                this.sBuilder.closeOutAllSessions(true);
            }
        }

    }

    /**
     * New holder.
     * 
     * @return the holder
     */
    public static Holder newHolder() {
        return new Holder();
    }

    /**
     * Sets the close out mode.
     * 
     * @param closeOutMode the new close out mode
     */
    public void setCloseOutMode(int closeOutMode) {
        this.closeOutMode = closeOutMode;
    }

}
