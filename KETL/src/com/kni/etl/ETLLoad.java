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
 * Insert the type's description here. Creation date: (3/5/2002 3:41:34 PM)
 * 
 * @author: Administrator
 */
public class ETLLoad {

    /** The Load ID. */
    public int LoadID;

    /** The start_job_id. */
    public String start_job_id;
    
    /** The start_date. */
    public java.util.Date start_date;
    
    /** The project_id. */
    public int project_id;
    
    /** The end_date. */
    public java.util.Date end_date;
    
    /** The ignored_parents. */
    public boolean ignored_parents;
    
    /** The failed. */
    public boolean failed;
    
    /** The running. */
    public boolean running = false;
    
    /** The job execution ID. */
    public int jobExecutionID; // this is used when getting all loads for a particular job

    /**
     * ETLLoad constructor comment.
     */
    public ETLLoad() {
        super();
    }
}
