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
package com.kni.etl.ketl.kernel;

import com.kni.etl.ketl.exceptions.KETLError;


// TODO: Auto-generated Javadoc
/**
 * A factory for creating Kernel objects.
 */
public class KernelFactory {
    
    /**
     * Gets the new kernel.
     * 
     * @return the new kernel
     * 
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     */
    public static KETLKernel getNewKernel() throws InstantiationException, IllegalAccessException {
        
        Class cl ;
        try {
             cl = Class.forName("com.kni.etl.ketl.kernel.KETLKernelImpl");                       
        } catch (ClassNotFoundException e) {
            throw new KETLError("KETL Kernel not found, please download the GPL server component");
        }
        
        KETLKernel ke = (KETLKernel) cl.newInstance();
        
        return ke;
    }
}
