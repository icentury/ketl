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
package com.kni.etl.ketl.transformation;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
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

    /**
     * Instantiates a new filter transformation.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public FilterTransformation(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLTransform#getRecordExecuteMethodHeader()
     */
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

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
     */
    @Override
    protected void close(boolean success) {
        // TODO Auto-generated method stub

    }

}
