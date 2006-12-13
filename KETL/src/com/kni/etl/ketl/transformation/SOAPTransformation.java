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
        String user = this.getParameterValue(0, USER_ATTRIB);
        String password = this.getParameterValue(0, PASSWORD_ATTRIB);

        if (user != null) {
            call.setUsername(user);
            call.setPassword(password);
        }
    }

    protected void setupService() throws KETLThreadException, MalformedURLException, ServiceException, AxisFault,
            RemoteException {
        // startup code

        this.getParamaterLists(XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
                EngineConstants.PARAMETER_LIST, null));

        try {
            mURL = new URL(this.getParameterValue(0, SOAPURL_ATTRIB));
        } catch (MalformedURLException e) {
            throw new KETLThreadException("Could not connect to URL " + this.getParameterValue(0, SOAPURL_ATTRIB), e, this);
        }
        mNamespace = this.getParameterValue(0, NAMESPACE_ATTRIB);
        mMethod = this.getParameterValue(0, METHOD_ATTRIB);
        String wsdl = this.getParameterValue(0, WSDL_ATTRIB);
        if (XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), TYPE_ATTRIB, SOAP_TYPES[SOAP_RPC])
                .equalsIgnoreCase(SOAP_TYPES[SOAP_RPC])) {
            mSOAPType = SOAP_RPC;
        }
        else
            mSOAPType = SOAP_DOC;

        if (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(),
                SOAPConnection.DISABLEHOSTNAME_VERIFICATION_ATTRIB, false)) {
            class MyHostnameVerifier implements javax.net.ssl.HostnameVerifier {

                public boolean verify(String arg0, SSLSession arg1) {
                    System.out.println("Warning: URL Host: " + arg0 + " vs. " + arg1.getPeerHost());
                    return true;
                }

            }

            mOriginalHostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

            HttpsURLConnection.setDefaultHostnameVerifier(new MyHostnameVerifier());

        }

        String targetNamespace = this.getParameterValue(0, TARGET_NAMESPACE_ATTRIB);
        QName service = targetNamespace == null ? new QName(this.getParameterValue(0, SERVICENAME_ATTRIB)) : new QName(
                targetNamespace, this.getParameterValue(0, SERVICENAME_ATTRIB));

        mService = new Service(new URL(wsdl), service);
        Iterator it = mService.getPorts();
        StringBuilder sb = new StringBuilder();
        boolean multiplePorts = false;
        while (it.hasNext()) {
            if (mDefaultPort == null) {
                mDefaultPort = (QName) it.next();
                sb.append(mDefaultPort.toString());
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
        QName method = mNamespace == null ? new QName(mMethod) : new QName(mNamespace, mMethod);

        call = (Call) mService.createCall(mDefaultPort, method);

        ResourcePool.LogMessage(toString(), ResourcePool.INFO_MESSAGE, "Found web service " + call.getOperationName());

        call.setTargetEndpointAddress(mURL);
        call.setOperationStyle(SOAP_TYPES[mSOAPType]);

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
            QName param = mNamespace == null ? new QName(this.mstrName) : new QName(mNamespace, this.mstrName);

            ParameterDesc pd = call.getOperation().getParamByQName(param);
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
                QName param = mNamespace == null ? new QName(this.mstrName) : new QName(mNamespace, this.mstrName);

                ArrayList res = call.getOperation().getOutParams();
                if (res.size() > 0) {
                    ParameterDesc pd = call.getOperation().getParamByQName(param);
                    if (pd != null) {
                        this.parameterPos = pd.getOrder();
                        this.getXMLConfig().setAttribute("DATATYPE", pd.getJavaType().getCanonicalName());
                        this.setPortClass();

                    }
                }
                else {
                    this.getXMLConfig().setAttribute("DATATYPE",
                            call.getOperation().getReturnClass().getCanonicalName());
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
                ((Element) this.getXMLConfig()).setAttribute("CHANNEL", "DEFAULT");

            mstrName = XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), NAME_ATTRIB, null);

            if (this.getCode() == null) {
                this.parameter = true;
            }
            else if (this.isConstant() == false && this.containsCode() == false) {
                ETLPort port = this.getAssociatedInPort();

                if (mstrName == null)
                    ((Element) this.getXMLConfig()).setAttribute("NAME", port.mstrName);
            }

            return this.mstrName;
        }

    }

    public SOAPTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

        try {
            setupService();
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

        for (int x = 0; x < this.mInPorts.length; x++) {

            if (((SOAPETLInPort) this.mInPorts[x]).parameter)
                pos++;

        }

        callMsg = new Object[pos];

        return 0;
    }

    Object[] items;

    public void callMethod(Object[] pInputData) throws KETLTransformException {

        try {

            for (int i = 0; i < this.mInPorts.length; i++) {
                SOAPETLInPort port = (SOAPETLInPort) this.mInPorts[i];

                if (port.parameter) {
                    callMsg[port.parameterPos] = pInputData[port.getSourcePortIndex()];
                }
            }

            Object resp = call.invoke(callMsg);

            if (resp instanceof java.rmi.RemoteException) {
                throw new KETLWriteException((Exception) resp);
            }

            List ls = call.getOutputValues();
            items = ls.toArray();

        } catch (AxisFault e) {
            String req = "N/A", res = "N/A";
            try {
                req = call.getMessageContext().getRequestMessage().getSOAPPartAsString();
            } catch (Exception e1) {
            }
            try {
                res = call.getMessageContext().getCurrentMessage().getSOAPPartAsString();
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
                        pOutputData[i] = items[port.parameterPos == -1 ? 0 : port.parameterPos];
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
