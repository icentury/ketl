package com.kni.etl.ketl.ws;


import javax.naming.InitialContext;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.kni.etl.XMLMetadataBridge;

public class KETLConfigure implements ServletContextListener {

    ServletContext servletContext;
   
    InitialContext _initialContext;

    public void contextInitialized(ServletContextEvent sce) {
        servletContext = sce.getServletContext();
        try {
            _initialContext = new InitialContext();
            
            String ketlPath = (String) _initialContext.lookup("java:comp/env/KETLPath");
            String ketlServersFile = (String) _initialContext.lookup("java:comp/env/KETLServersFile");
            XMLMetadataBridge.configure(ketlPath,ketlServersFile);

        } catch (Throwable ex) {
            ex.printStackTrace();
        }

    }
    

   
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            _initialContext.close();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}
