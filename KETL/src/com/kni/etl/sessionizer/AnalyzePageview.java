/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

/**
 * @author nwakefield Creation Date: Jun 17, 2003
 */
public class AnalyzePageview implements Serializable {

    private static final long serialVersionUID = 1L;

    public final static int RESTART_SESSIONS_NO_STORE = 1;
    public final static int DISABLE_RESTART_NO_STORE = 2;
    public final static int DISABLE_RESTART_AND_STORE = 3;
    public final static int RESTART_SESSIONS_AND_STORE = 4;

    transient PageViewItemFinderAccelerator pageViewItemAccelerator = null;
    transient PageParserPageDefinition[] pDef = null;
    transient SessionDefinition sessionDef = null;
    int closeOutMode = AnalyzePageview.DISABLE_RESTART_AND_STORE;
    boolean mbPagesOnly = false;
    SessionBuilder sBuilder = null;

    static public class Holder {
        public Session currentSession;
        public Object[] pageView;
        public boolean isPageView;
        public boolean bCleansed;
        public short iCleansedID;
        public short iPageSequence;
        public int iAssociatedHits;
    }
    
    public Holder analyzeHit(Holder holder) throws Exception {
        Holder res = this.sBuilder.analyzeHit(holder);

        if(res == null && this.sBuilder.ignoreHit)
            return null;
        
        if (res == null || res.currentSession ==null) {
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

    transient IDCounter iBadSessions = new IDCounter();
    
    /**
     * @param resultRecord
     */
    private void outputBadSession(Object[] resultRecord) {
        this.iBadSessions.incrementID();
        ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE, "Invalid pagev view:"
                + java.util.Arrays.toString(resultRecord));
    }

    public void configure(IDCounter idCounter,SessionDefinition sessionDef, PageParserPageDefinition[] pDef, boolean pPagesOnly,
            boolean hitCountRequired, int[] itemMap, List completeSessionList) {

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

    public void close(boolean fatalError) {

        // mark remaining sessions done or still open
        if (fatalError == false) {
            if (this.closeOutMode == AnalyzePageview.DISABLE_RESTART_AND_STORE) {
                // store last hit date last activity
                this.sBuilder.closeOutAllSessions(false);
            }
            else if (this.closeOutMode == AnalyzePageview.RESTART_SESSIONS_AND_STORE) { // store null for last activity, makes
                // identification of non closed
                // sessions easier
                this.sBuilder.closeOutAllSessions(true);
            }
        }

    }

    public static Holder newHolder() {
        return new Holder();
    }

    
    public void setCloseOutMode(int closeOutMode) {
        this.closeOutMode = closeOutMode;
    }

}
