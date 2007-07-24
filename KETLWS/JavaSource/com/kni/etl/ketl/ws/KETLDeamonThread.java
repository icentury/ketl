package com.kni.etl.ketl.ws;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.kernel.KETLKernel;
import com.kni.etl.ketl.kernel.KernelFactory;
import com.kni.etl.Metadata;

/** Start the KETL Deamon in separate thread.
 *  Used when starting up the web service to also start the kernel.
 *  6/29/2007 Dao Nguyen - creator
 */
public class KETLDeamonThread extends Thread {
    private boolean isRunning = false;
    private KETLKernel ke = null;
    
    private java.lang.String[] args;
    private String ketlPath = "";

    public boolean isRunning() {
        return isRunning;
    }
    public String mdServerName() {
        return "";
    }
    
    public KETLDeamonThread (String pketlPath, String pKetlServerFile, String pKetlMetadataServer, boolean runInBackground) {
        if (pKetlMetadataServer==null || pKetlMetadataServer.length()==0) 
            args = new String[2];
        else {
            args = new String[3];
            args[2] = "SERVERNAME=" + pKetlMetadataServer;
        }
        // reset the path to find this system.xml file 
        args[0] = "APP_PATH=" + pketlPath;
        // the default metadata server in the metadata config file (e.g. KETLServers.xml) will be used
        args[1] = "CONFIG=" + pketlPath + "\\" + pKetlServerFile;
        ketlPath = pketlPath;

        if (runInBackground) {
            Thread t = new Thread(this);
            // What priority should we set for the deamon?
            t.setPriority(Thread.MIN_PRIORITY);
            t.start();
          } else {
            run();
          }
    }
    
    public void run() throws RuntimeException {
        try {
            //we need to point to the right system.xml file before it is read by the kernel (on startup?) 
            Metadata.setKETLPath(ketlPath);
            ResourcePool.setCacheIndexPrefix("Daemon");
            ke = KernelFactory.getNewKernel();
            isRunning = true;
            ke.run(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            isRunning = false;
        }
    }

}
