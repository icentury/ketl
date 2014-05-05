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
package com.kni.etl.ketl;




// TODO: Auto-generated Javadoc
/**
 * The Interface SOAPConnection.
 * 
 * @author nwakefield
 * 
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface SOAPConnection {
    
    /** The Constant SOAPURL_ATTRIB. */
    public static final String SOAPURL_ATTRIB = "SOAPURL";
    
    /** The Constant METHOD_ATTRIB. */
    public static final String METHOD_ATTRIB = "METHOD";
    
    /** The Constant PASSWORD_ATTRIB. */
    public static final String PASSWORD_ATTRIB = "PASSWORD";
    
    /** The Constant DISABLEHOSTNAME_VERIFICATION_ATTRIB. */
    public static final String DISABLEHOSTNAME_VERIFICATION_ATTRIB = "DISABLEHOSTVERIFICATION";
    
    /** The Constant USER_ATTRIB. */
    public static final String USER_ATTRIB = "USER";    
    
    /** The Constant NAMESPACE_ATTRIB. */
    public static final String NAMESPACE_ATTRIB = "NAMESPACE";  
    
    /** The Constant TARGET_NAMESPACE_ATTRIB. */
    public static final String TARGET_NAMESPACE_ATTRIB = "TARGETNAMESPACE";  
    
    /** The Constant SERVICENAME_ATTRIB. */
    public static final String SERVICENAME_ATTRIB = "SERVICENAME";  
    
    /** The Constant TYPE_ATTRIB. */
    public static final String TYPE_ATTRIB = "TYPE";
    
    /** The Constant WSDL_ATTRIB. */
    public static final String WSDL_ATTRIB = "WSDL";
    
    
    /** The SOA p_ TYPES. */
    public static String[] SOAP_TYPES = {"rpc","document"};
    
    /** The Constant SOAP_RPC. */
    public static final int SOAP_RPC = 0;
    
    /** The Constant SOAP_DOC. */
    public static final int SOAP_DOC = 1;
    
    
    

}
