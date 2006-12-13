package com.kni.etl.ketl.kernel;

import com.kni.etl.ketl.exceptions.KETLError;


public class KernelFactory {
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
