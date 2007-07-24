package com.kni.etl.ketl.ws;


import javax.naming.InitialContext;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.kni.etl.XMLMetadataBridge;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.Metadata;

public class KETLConfigure implements ServletContextListener {

    ServletContext servletContext;
    InitialContext _initialContext;

    String ketlPath = "";
    String ketlServersFile = "";
    String ketlMDSrvr = "";
    
    KETLDeamonThread wsDeamon = null;

    /* (non-Javadoc)
     * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent sce) {
        
        servletContext = sce.getServletContext();
        try {
            _initialContext = new InitialContext();
            
            ketlPath = (String) _initialContext.lookup("java:comp/env/KETLPath");
            ketlServersFile = (String) _initialContext.lookup("java:comp/env/KETLServersFile");
            XMLMetadataBridge.configure(ketlPath,ketlServersFile);

            // this starts the deamon (kernel) in a separate thread
            ketlMDSrvr = (String) _initialContext.lookup("java:comp/env/KETLMDServer");
            if ( ketlMDSrvr!=null && ketlMDSrvr.length()>0 ) 
                wsDeamon = new KETLDeamonThread(ketlPath, ketlServersFile, ketlMDSrvr, true); 

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

   
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            // to shut down the deamon cleanly, use the methods of the Metadata class
            Metadata md = ResourcePool.getMetadata();
            md.shutdownServer(ketlMDSrvr, false);

            // this.wait(1000);
            // waits for the other thread to terminate or ~5 second max (depending upon OS)
            wsDeamon.join(5000);               
            if (wsDeamon.isRunning()) 
                System.out.println("Info: the deamon is still shuting down...");
         
            _initialContext.close();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}
