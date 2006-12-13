/*
 * Created on Apr 5, 2006
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl;




/**
 * @author nwakefield
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface SOAPConnection {
    public static final String SOAPURL_ATTRIB = "SOAPURL";
    public static final String METHOD_ATTRIB = "METHOD";
    public static final String PASSWORD_ATTRIB = "PASSWORD";
    public static final String DISABLEHOSTNAME_VERIFICATION_ATTRIB = "DISABLEHOSTVERIFICATION";
    public static final String USER_ATTRIB = "USER";    
    public static final String NAMESPACE_ATTRIB = "NAMESPACE";  
    public static final String TARGET_NAMESPACE_ATTRIB = "TARGETNAMESPACE";  
    public static final String SERVICENAME_ATTRIB = "SERVICENAME";  
    public static final String TYPE_ATTRIB = "TYPE";
    public static final String WSDL_ATTRIB = "WSDL";
    
    
    public static String[] SOAP_TYPES = {"rpc","document"};
    public static final int SOAP_RPC = 0;
    public static final int SOAP_DOC = 1;
    
    
    

}
