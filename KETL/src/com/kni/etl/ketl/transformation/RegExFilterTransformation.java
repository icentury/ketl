/*
 * Created on Jul 13, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package com.kni.etl.ketl.transformation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// Create a parallel transformation. All thread management is done for you
// the parallism is within the transformation

public class RegExFilterTransformation extends ETLTransformation {

    private static final String REGEXPR_ATTRIB = "REGEXPR";

    public RegExFilterTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

    }

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new RegExFilterOutPort(this, srcStep);
    }

    class RegExFilterOutPort extends ETLOutPort {

        Matcher matcher;
        boolean nullMatches = false;

        @Override
        public int initialize(Node xmlConfig) throws ClassNotFoundException, KETLThreadException {
            String regexID = XMLHelper.getAttributeAsString(xmlConfig.getAttributes(),
                    RegExFilterTransformation.REGEXPR_ATTRIB, null);

            if (regexID != null) {
                String regex = XMLHelper.getChildNodeValueAsString(xmlConfig.getParentNode(), "REGEXPR", "ID", regexID,
                        null);
                if (regex != null) {

                    this.nullMatches = XMLHelper.getAttributeAsBoolean(xmlConfig.getAttributes(), "NULLMATCHES",
                            this.nullMatches);

                    Pattern pattern = Pattern.compile(regex);
                    this.matcher = pattern.matcher("");
                }
            }

            String tmp = XMLHelper.getTextContent(xmlConfig);

            if (tmp == null || tmp.length() == 0)
                xmlConfig.setTextContent(EngineConstants.VARIABLE_PARAMETER_START
                        + XMLHelper.getAttributeAsString(xmlConfig.getAttributes(), "NAME", null)
                        + EngineConstants.VARIABLE_PARAMETER_END);

            int res = super.initialize(xmlConfig);

            return res;

        }

        @Override
        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            String tmp = super.generateCode(portReferenceIndex);

            if (this.matcher == null)
                return tmp;

            return tmp + ";if(((" + this.mesStep.getClass().getCanonicalName() + ")this.getOwner()).skipRecord("
                    + this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "],"
                    + this.mesStep.getUsedPortIndex(this) + ")) return SKIP_RECORD";
        }

        public RegExFilterOutPort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    public boolean skipRecord(Object datum, int portIdx) {

        Matcher regex = ((RegExFilterOutPort) this.mOutPorts[portIdx]).matcher;

        if (datum == null) {
            if (((RegExFilterOutPort) this.mOutPorts[portIdx]).nullMatches == false)
                return true;

            return false;
        }
        regex.reset(datum.toString());
        if (regex.find() == false)
            return true;

        return false;
    }

    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        StringBuilder sb = new StringBuilder(super.getRecordExecuteMethodHeader());

        Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "FILTER", "*", "*");

        if (nl != null) {
            for (Node element : nl) {

                String code = XMLHelper.getTextContent(element);

                if (code == null || code.length() == 0)
                    throw new KETLThreadException("Filter tag requires an expression", this);

                String[] parms = EngineConstants.getParametersFromText(code);

                for (String element0 : parms) {
                    ETLInPort port = this.getInPort(element0);
                    code = EngineConstants.replaceParameter(code, element0, port.generateReference());
                }

                sb.append("if(!(" + code + ")) return SKIP_RECORD;");

            }
        }

        return sb.toString();
    }

    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub

    }

}
