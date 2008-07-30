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
package com.kni.etl.ketl.writer;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import org.apache.axis.AxisFault;
import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.description.ParameterDesc;
import org.w3c.dom.Node;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.SOAPConnection;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class SOAPWriter.
 */
public class SOAPWriter extends ETLWriter implements DefaultWriterCore, SOAPConnection {

    /** The call. */
    Call call = null;
    
    /** The call msg. */
    Object callMsg[];
    
    /** The dumped params. */
    boolean dumpedParams = false;
    
    /** The default port. */
    QName mDefaultPort = null;
    
    /** The method. */
    String mMethod;
    
    /** The namespace. */
    String mNamespace;
    
    /** The original hostname verifier. */
    javax.net.ssl.HostnameVerifier mOriginalHostnameVerifier = null;
    
    /** The service. */
    Service mService;
    
    /** The SOAP type. */
    int mSOAPType;
    
    /** The URL. */
    URL mURL;
    
    /** The parameter types. */
    Class[] parameterTypes;

    /**
     * Instantiates a new SOAP writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public SOAPWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /**
     * Authenticate call.
     * 
     * @throws MalformedURLException the malformed URL exception
     * @throws ServiceException the service exception
     * @throws AxisFault the axis fault
     * @throws RemoteException the remote exception
     */
    protected void authenticateCall() throws MalformedURLException, ServiceException, AxisFault, RemoteException {
        String user = this.getParameterValue(0, SOAPConnection.USER_ATTRIB);
        String password = this.getParameterValue(0, SOAPConnection.PASSWORD_ATTRIB);

        if (user != null) {
            this.call.setUsername(user);
            this.call.setPassword(password);
        }
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLStep#complete()
     */
    @Override
    public int complete() throws KETLThreadException {

        int res = super.complete();

        if (this.mOriginalHostnameVerifier != null) {
            HttpsURLConnection.setDefaultHostnameVerifier(this.mOriginalHostnameVerifier);
        }

        return res;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.ETLStep#initialize(org.w3c.dom.Node)
     */
    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {

        int res = super.initialize(xmlConfig);
        if (res != 0)
            return res;

        try {
            this.setupService();
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }

        this.parameterTypes = new Class[this.mInPorts.length];
        for (int x = 0; x < this.mInPorts.length; x++) {

            QName param = this.mNamespace == null ? new QName(this.mInPorts[x].mstrName) : new QName(this.mNamespace,
                    this.mInPorts[x].mstrName);

            ParameterDesc pd = this.call.getOperation().getParamByQName(param);
            if (pd == null) {
                ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Parameter " + this.mInPorts[x].mstrName
                        + " not in service definition and will be ignored");
                if (this.dumpedParams == false) {
                    ArrayList params = this.call.getOperation().getParameters();

                    String str = "Expected parameters:\n";
                    for (int i = 0; i < params.size(); i++) {
                        ParameterDesc p = (ParameterDesc) params.get(i);
                        if (i > 0) {
                            str += "\n";
                        }
                        if (p != null)
                            str += p.toString();
                    }
                    ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, str);
                    this.dumpedParams = true;
                }
            }
            else
                this.parameterTypes[x] = pd.getJavaType();
        }

        this.callMsg = new Object[this.mInPorts.length];

        return 0;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[], java.lang.Class[], int)
     */
    public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
            throws KETLWriteException {
        for (int x = 0; x < pRecordWidth; x++) {
            if (this.parameterTypes[x] != null)
                this.callMsg[x] = this.mInPorts[x].isConstant() ? this.mInPorts[x].getConstantValue()
                        : pInputRecords[this.mInPorts[x].getSourcePortIndex()];
        }

        try {
            Object resp = this.call.invoke(this.callMsg);

            if (resp instanceof java.rmi.RemoteException) {
                throw new KETLWriteException((Exception) resp);
            }

            try {
                List ls = this.call.getOutputValues();

                if (ls != null) {
                    Object[] items = ls.toArray();

                    if (items != null && items.length > 0) {
                        ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "SOAP call returned " + items.length
                                + " value(s) " + java.util.Arrays.toString(items));
                    }
                }

            } catch (Exception e) {
            }

        } catch (AxisFault e) {
            String req = "N/A", res = "N/A";
            try {
                req = this.call.getMessageContext().getRequestMessage().getSOAPPartAsString();
            } catch (Exception e1) {
            }
            try {
                res = this.call.getMessageContext().getCurrentMessage().getSOAPPartAsString();
            } catch (Exception e1) {
            }

            throw new KETLWriteException(
                    "SOAP web service call failed\n\tCode:" + e.getFaultCode() + "\n\tActor:" + e.getFaultActor()
                            + "\n\tReason:" + e.getFaultReason() + "\n\tRole:" + e.getFaultRole() + "\n\tNode:"
                            + e.getFaultNode() + "\n\tXML SOAP request: " + req + "\n\tXML SOAP response: " + res, e);

        } catch (Exception e) {
            throw new KETLWriteException(e);
        }

        return 1;
    }

    /**
     * Setup service.
     * 
     * @throws KETLThreadException the KETL thread exception
     * @throws MalformedURLException the malformed URL exception
     * @throws ServiceException the service exception
     * @throws AxisFault the axis fault
     * @throws RemoteException the remote exception
     */
    protected void setupService() throws KETLThreadException, MalformedURLException, ServiceException, AxisFault,
            RemoteException {
        // startup code
        try {
            this.mURL = new URL(this.getParameterValue(0, SOAPConnection.SOAPURL_ATTRIB));
        } catch (MalformedURLException e) {
            throw new KETLThreadException("Could not connect to URL "
                    + this.getParameterValue(0, SOAPConnection.SOAPURL_ATTRIB), e, this);
        }
        this.mNamespace = this.getParameterValue(0, SOAPConnection.NAMESPACE_ATTRIB);
        this.mMethod = this.getParameterValue(0, SOAPConnection.METHOD_ATTRIB);
        String wsdl = this.getParameterValue(0, SOAPConnection.WSDL_ATTRIB);
        if (XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), SOAPConnection.TYPE_ATTRIB,
                SOAPConnection.SOAP_TYPES[SOAPConnection.SOAP_RPC]).equalsIgnoreCase(
                SOAPConnection.SOAP_TYPES[SOAPConnection.SOAP_RPC])) {
            this.mSOAPType = SOAPConnection.SOAP_RPC;
        }
        else
            this.mSOAPType = SOAPConnection.SOAP_DOC;

        if (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(),
                SOAPConnection.DISABLEHOSTNAME_VERIFICATION_ATTRIB, false)) {
            class MyHostnameVerifier implements javax.net.ssl.HostnameVerifier {

                public boolean verify(String arg0, SSLSession arg1) {
                    ResourcePool.logMessage("Warning: URL Host: " + arg0 + " vs. " + arg1.getPeerHost());
                    return true;
                }

            }

            this.mOriginalHostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

            HttpsURLConnection.setDefaultHostnameVerifier(new MyHostnameVerifier());

        }

        String targetNamespace = this.getParameterValue(0, SOAPConnection.TARGET_NAMESPACE_ATTRIB);
        QName service = targetNamespace == null ? new QName(this
                .getParameterValue(0, SOAPConnection.SERVICENAME_ATTRIB)) : new QName(targetNamespace, this
                .getParameterValue(0, SOAPConnection.SERVICENAME_ATTRIB));

        this.mService = new Service(new URL(wsdl), service);
        Iterator it = this.mService.getPorts();
        StringBuilder sb = new StringBuilder();
        boolean multiplePorts = false;
        while (it.hasNext()) {
            if (this.mDefaultPort == null) {
                this.mDefaultPort = (QName) it.next();
                sb.append(this.mDefaultPort.toString());
            }
            else {
                multiplePorts = true;
                sb.append(',');
                sb.append(it.next().toString());
            }
        }

        if (multiplePorts) {
            throw new KETLThreadException("ERROR: Multiple web service ports found, please specify one: "
                    + sb.toString(), this);
        }

        // generate SOAP call
        QName method = this.mNamespace == null ? new QName(this.mMethod) : new QName(this.mNamespace, this.mMethod);

        this.call = (Call) this.mService.createCall(this.mDefaultPort, method);

        ResourcePool.LogMessage(this.toString(), ResourcePool.INFO_MESSAGE, "Found web service "
                + this.call.getOperationName());

        this.call.setTargetEndpointAddress(this.mURL);
        this.call.setOperationStyle(SOAPConnection.SOAP_TYPES[this.mSOAPType]);

        this.authenticateCall();
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {

    }

    /**
     * Gets the call.
     * 
     * @return the call
     */
    final protected Call getCall() {
        return this.call;
    }

    /**
     * Gets the namespace.
     * 
     * @return the namespace
     */
    final public String getNamespace() {
        return this.mNamespace;
    }

}
