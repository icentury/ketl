/*
 * Copyright (c) 2006 Kinetic Networks, Inc. All Rights Reserved.
 * Created on Jul 6, 2006
 * 
 */
package com.kni.etl.ketl;

import java.util.concurrent.BlockingQueue;

/**
 * @author nwakefield
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface ETLWriteQueue {

    public abstract BlockingQueue getWriteQueue();

}