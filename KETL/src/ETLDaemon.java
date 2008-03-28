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
import java.io.File;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.kernel.KETLKernel;
import com.kni.etl.ketl.kernel.KernelFactory;
import com.kni.util.ExternalJarLoader;

// TODO: Auto-generated Javadoc
/**
 * Insert the type's description here. Creation date: (4/28/2002 11:57:07 AM)
 * 
 * @author: Administrator
 */
class ETLDaemon {

    /**
     * Main.
     * 
     * @param args the args
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     */
    public static void main(java.lang.String[] args) throws InstantiationException, IllegalAccessException {
    	String ketldir = System.getenv("KETLDIR");
		if (ketldir == null) {
			ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.WARNING_MESSAGE,"KETLDIR not set, defaulting to working dir");
			ketldir = ".";
		}

		ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf" + File.separator + "Extra.Libraries"),
				"ketlextralibs", ";");
		
        ResourcePool.setCacheIndexPrefix("Daemon");
        KETLKernel ke = KernelFactory.getNewKernel();
        ke.run(args);
    }
}
