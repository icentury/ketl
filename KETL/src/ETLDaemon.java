/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.kernel.KETLKernel;
import com.kni.etl.ketl.kernel.KernelFactory;

/**
 * Insert the type's description here. Creation date: (4/28/2002 11:57:07 AM)
 * 
 * @author: Administrator
 */
class ETLDaemon {

    
    public static void main(java.lang.String[] args) throws InstantiationException, IllegalAccessException {
        ResourcePool.setCacheIndexPrefix("Daemon");
        KETLKernel ke = KernelFactory.getNewKernel();        
        ke.run(args);
    }
}
