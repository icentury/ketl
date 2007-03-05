/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;

/**
 * Insert the type's description here. Creation date: (3/5/2002 3:41:34 PM)
 * 
 * @author: Administrator
 */
public class ETLLoad {

    public int LoadID;

    public String start_job_id;
    public java.util.Date start_date;
    public int project_id;
    public java.util.Date end_date;
    public boolean ignored_parents;
    public boolean failed;
    public boolean running = false;
    public int jobExecutionID; // this is used when getting all loads for a particular job

    /**
     * ETLLoad constructor comment.
     */
    public ETLLoad() {
        super();
    }
}
