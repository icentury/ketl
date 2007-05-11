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
 * Insert the type's description here. Creation date: (5/3/2002 1:13:01 PM)
 * 
 * @author: Administrator
 */
public class ETLJobManagerStatus extends ETLStatus {

    /** The astr status messages. */
    private static java.lang.String[] astrStatusMessages = { "Initializing", "Ready", "Queuing", "Full", "Error",
            "Shutting Down", "Terminated" };
    
    /** The Constant INITIALIZING. */
    public final static int INITIALIZING = 0; // Starting up threads
    
    /** The Constant READY. */
    public final static int READY = 1; // At least one thread available for immediate job processing
    
    /** The Constant QUEUEING. */
    public final static int QUEUEING = 2; // All threads busy, but there is space in the queue for more jobs
    
    /** The Constant FULL. */
    public final static int FULL = 3; // All threads busy and the queue is at it's maximum
    
    /** The Constant ERROR. */
    public final static int ERROR = 4; // Unable to start up, or all threads are dead
    
    /** The Constant SHUTTING_DOWN. */
    public final static int SHUTTING_DOWN = 5; // Shutting down threads
    
    /** The Constant TERMINATED. */
    public final static int TERMINATED = 6; // Done shutting down threads

    /**
     * ETLJobExecutorStatus constructor comment.
     */
    public ETLJobManagerStatus() {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:13:31 AM)
     * 
     * @return java.lang.String[]
     */
    @Override
    public String[] getStatusMessages() {
        return ETLJobManagerStatus.astrStatusMessages;
    }
}
