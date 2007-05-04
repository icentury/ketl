/*
 * Created on Jul 13, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.transformation;

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
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.SOAPConnection;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

public class SOAPTransformation extends ETLTransformation implements SOAPConnection {

    Call call = null;
    Object callMsg[];
    boolean dumpedParams = false;
    QName mDefaultPort = null;
    String mMethod;
    String mNamespace;
    javax.net.ssl.HostnameVerifier mOriginalHostnameVerifier = null;
    Service mService;
    int mSOAPType;
    URL mURL;
    Class[] parameterTypes;

    protected void authenticateCall() throws MalformedURLException, ServiceException, AxisFault, RemoteException {
        String user = this.getParameterValue(0, SOAPConnection.USER_ATTRIB);
        String password = this.getParameterValue(0, SOAPConnection.PASSWORD_ATTRIB);

        if (user != null) {
            this.call.setUsername(user);
            this.call.setPassword(password);
        }
    }

    protected void setupService() throws KETLThreadException, MalformedURLException, ServiceException, AxisFault,
            RemoteException {
        // startup code

        this.getParamaterLists(XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
                EngineConstants.PARAMETER_LIST, null));

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
                    System.out.println("Warning: URL Host: " + arg0 + " vs. " + arg1.getPeerHost());
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

    class SOAPETLInPort extends ETLInPort {

        public SOAPETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            if (super.initialize(xmlConfig) != 0)
                return -1;
            QName param = SOAPTransformation.this.mNamespace == null ? new QName(this.mstrName) : new QName(
                    SOAPTransformation.this.mNamespace, this.mstrName);

            ParameterDesc pd = SOAPTransformation.this.call.getOperation().getParamByQName(param);
            if (pd != null) {
                this.parameter = true;
                this.parameterPos = pd.getOrder();
                this.used(true);
            }
            return 0;
        }

        boolean parameter = false;
        int parameterPos;

    }

    class SOAPETLOutPort extends ETLOutPort {

        @Override
        public ETLPort getAssociatedInPort() throws KETLThreadException {
            if (this.parameter)
                return this;
            return super.getAssociatedInPort();
        }

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            if (super.initialize(xmlConfig) != 0)
                return -1;

            if (this.parameter) {
                QName param = SOAPTransformation.this.mNamespace == null ? new QName(this.mstrName) : new QName(
                        SOAPTransformation.this.mNamespace, this.mstrName);

                ArrayList res = SOAPTransformation.this.call.getOperation().getOutParams();
                if (res.size() > 0) {
                    ParameterDesc pd = SOAPTransformation.this.call.getOperation().getParamByQName(param);
                    if (pd != null) {
                        this.parameterPos = pd.getOrder();
                        this.getXMLConfig().setAttribute("DATATYPE", pd.getJavaType().getCanonicalName());
                        this.setPortClass();

                    }
                }
                else {
                    this.getXMLConfig().setAttribute("DATATYPE",
                            SOAPTransformation.this.call.getOperation().getReturnClass().getCanonicalName());
                    this.parameterPos = -1;
                }

                this.setPortClass();
            }

            return 0;
        }

        public SOAPETLOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

        boolean parameter = false;
        int parameterPos;

        @Override
        public String getPortName() throws DOMException, KETLThreadException {
            if (this.mstrName != null)
                return this.mstrName;

            if (XMLHelper.getElementsByName(this.getXMLConfig(), "OUT", "CHANNEL", null) == null)
                (this.getXMLConfig()).setAttribute("CHANNEL", "DEFAULT");

            this.mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), ETLPort.NAME_ATTRIB,
                    null);

            if (this.getCode() == null) {
                this.parameter = true;
            }
            else if (this.isConstant() == false && this.containsCode() == false) {
                ETLPort port = this.getAssociatedInPort();

                if (this.mstrName == null)
                    (this.getXMLConfig()).setAttribute("NAME", port.mstrName);
            }

            return this.mstrName;
        }

    }

    public SOAPTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        try {
            this.setupService();
        } catch (Exception e) {
            throw new KETLThreadException(e, this);
        }
    }

    @Override
    protected ETLInPort getNewInPort(ETLStep srcStep) {
        return new SOAPETLInPort(this, srcStep);
    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new SOAPETLOutPort(this, srcStep);
    }

    boolean mDumpSOAPFields = true;

    @Override
    protected int initialize(Node xmlConfig) throws KETLThreadException {

        int res = super.initialize(xmlConfig);
        if (res != 0)
            return res;

        int pos = 0;

        for (ETLInPort element : this.mInPorts) {

            if (((SOAPETLInPort) element).parameter)
                pos++;

        }

        this.callMsg = new Object[pos];

        return 0;
    }

    Object[] items;

    public void callMethod(Object[] pInputData) throws KETLTransformException {

        try {

            for (ETLInPort element : this.mInPorts) {
                SOAPETLInPort port = (SOAPETLInPort) element;

                if (port.parameter) {
                    this.callMsg[port.parameterPos] = pInputData[port.getSourcePortIndex()];
                }
            }

            Object resp = this.call.invoke(this.callMsg);

            if (resp instanceof java.rmi.RemoteException) {
                throw new KETLWriteException((Exception) resp);
            }

            List ls = this.call.getOutputValues();
            this.items = ls.toArray();

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

            throw new KETLTransformException(
                    "SOAP web service call failed\n\tCode:" + e.getFaultCode() + "\n\tActor:" + e.getFaultActor()
                            + "\n\tReason:" + e.getFaultReason() + "\n\tRole:" + e.getFaultRole() + "\n\tNode:"
                            + e.getFaultNode() + "\n\tXML SOAP request: " + req + "\n\tXML SOAP response: " + res, e);

        } catch (Exception e) {
            if (e instanceof KETLTransformException)
                throw (KETLTransformException) e;

            throw new KETLTransformException(e);
        }

    }

    public void getResults(Object[] pOutputData) throws KETLTransformException {

        try {

            for (int i = 0; i < this.mOutPorts.length; i++) {
                SOAPETLOutPort port = (SOAPETLOutPort) this.mOutPorts[i];

                if (port.isUsed()) {
                    if (port.parameter) {
                        pOutputData[i] = this.items[port.parameterPos == -1 ? 0 : port.parameterPos];
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof KETLTransformException)
                throw (KETLTransformException) e;

            throw new KETLTransformException(e);
        }
    }

    @Override
    protected void close(boolean success) {
    }

    @Override
    protected String getRecordExecuteMethodFooter() {

        return "((" + this.getClass().getCanonicalName() + ")this.getOwner()).getResults(pOutputRecords);"
                + super.getRecordExecuteMethodFooter();
    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        return super.getRecordExecuteMethodHeader() + "((" + this.getClass().getCanonicalName()
                + ")this.getOwner()).callMethod(pInputRecords);";
    }

}
