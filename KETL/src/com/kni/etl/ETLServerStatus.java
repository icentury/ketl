/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl;


/**
 * Insert the type's description here.
 * Creation date: (5/7/2002 7:43:28 PM)
 * @author: Administrator
 */
public class ETLServerStatus extends ETLStatus
{
    public final static int SERVER_SHUTDOWN = 3;
    public final static int SERVER_SHUTTING_DOWN = 2;
    public final static int SERVER_ALIVE = 1;
    public final static int SERVER_KILLED = 5;
    public final static int PAUSED = 4;

    /**
     * ETLServerStatus constructor comment.
     */
    public ETLServerStatus()
    {
        super();
    }
}
