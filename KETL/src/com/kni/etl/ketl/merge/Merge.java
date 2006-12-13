package com.kni.etl.ketl.merge;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLMerge;
import com.kni.etl.ketl.smp.ETLMergeCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

public class Merge extends ETLMerge {

    @Override
    protected ETLOutPort getNewOutPort(ETLStep srcStep) {
        return new ETLMergePort(this, srcStep);
    }

    class ETLMergePort extends ETLOutPort {

        public String generateCode(int portReferenceIndex) throws KETLThreadException {

            if (this.isUsed() == false)
                return "";

            // must be pure code then do some replacing
            String baseCode = XMLHelper.getTextContent(this.getXMLConfig());

            if (baseCode == null || baseCode.length() == 0)
                baseCode = "null";
            else {
                String[] params = EngineConstants.getParametersFromText(baseCode);

                for (int i = 0; i < params.length; i++) {
                    ETLInPort inport = this.mesStep.getInPort(params[i]);

                    if (inport == null) {
                        // get from parameter list
                        throw new KETLThreadException("Source port " + params[i] + " for step "
                                + this.mesStep.getName() + " could not be found, has it been declared as an IN port", this);
                    }
                    else {
                        baseCode = EngineConstants.replaceParameter(baseCode, params[i], inport.generateReference());
                        return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this)
                                + "] = " + inport.getCodeGenerationReferenceObject() + "==null?null:" + baseCode;
                    }
                }
            }

            return this.getCodeGenerationReferenceObject() + "[" + this.mesStep.getUsedPortIndex(this) + "] = "
                    + baseCode;

        }

        public ETLMergePort(ETLStep esOwningStep, ETLStep esSrcStep) {
            super(esOwningStep, esSrcStep);
        }

    }

    public Merge(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    protected void close(boolean success) {
    }

    @Override
    protected String getRecordExecuteMethodFooter() {
        return " return pLeftInputRecords!=null&&pRightInputRecords!=null?" + ETLMergeCore.SUCCESS_ADVANCE_BOTH
                + ":(pLeftInputRecords==null?" + ETLMergeCore.SUCCESS_ADVANCE_RIGHT + ":"
                + ETLMergeCore.SUCCESS_ADVANCE_LEFT + ");}";
    }
}
