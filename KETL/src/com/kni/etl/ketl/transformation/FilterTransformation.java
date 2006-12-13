/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.transformation;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// import java.util.HashMap;

/**
 * <p>
 * Title: JDBCWriter
 * </p>
 * <p>
 * Description: Generates the next logical key for a table.
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Kinetic Networks
 * </p>
 * 
 * @author Brian Sullivan
 * @version 1.0
 */
public class FilterTransformation extends ETLTransformation {

    public FilterTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    
    @Override
    protected String getRecordExecuteMethodHeader() throws KETLThreadException {
        StringBuilder sb = new StringBuilder(super.getRecordExecuteMethodHeader());

        Node[] nl = XMLHelper.getElementsByName(this.getXMLConfig(), "FILTER", "*", "*");

        if (nl != null) {
            for (int i = 0; i < nl.length; i++) {

                String code = XMLHelper.getTextContent(nl[i]);
                
                if(code == null || code.length() == 0)
                    throw new KETLThreadException("Filter tag requires an expression", this);
                
                String[] parms = EngineConstants.getParametersFromText(code);

                for (int x = 0; x < parms.length; x++) {
                    ETLInPort port = this.getInPort(parms[x]);
                    code = EngineConstants.replaceParameter(code, parms[x], port.generateReference());
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
