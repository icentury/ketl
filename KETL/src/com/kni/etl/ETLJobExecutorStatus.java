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
 * Insert the type's description here. Creation date: (5/7/2002 10:50:31 AM)
 * 
 * @author: Administrator
 */
public class ETLJobExecutorStatus extends ETLStatus {

    /** The astr status messages. */
    private static java.lang.String[] astrStatusMessages = { "Initializing", "Ready", "Working", "Error",
            "Shutting Down", "Terminated" };
    
    /** The Constant INITIALIZING. */
    public final static int INITIALIZING = 0; // Starting up
    
    /** The Constant READY. */
    public final static int READY = 1; // Ready to receive a job
    
    /** The Constant WORKING. */
    public final static int WORKING = 2; // Currently processing a job
    
    /** The Constant ERROR. */
    public final static int ERROR = 3; // Error
    
    /** The Constant SHUTTING_DOWN. */
    public final static int SHUTTING_DOWN = 4; // Shutting down
    
    /** The Constant TERMINATED. */
    public final static int TERMINATED = 5; // Shutdown complete

    /**
     * ETLJobExecutorStatus constructor comment.
     */
    public ETLJobExecutorStatus() {
        super();
    }

    /**
     * Insert the method's description here. Creation date: (5/7/2002 11:07:53 AM)
     * 
     * @return java.lang.String[]
     */
    @Override
    public String[] getStatusMessages() {
        return ETLJobExecutorStatus.astrStatusMessages;
    }
}
