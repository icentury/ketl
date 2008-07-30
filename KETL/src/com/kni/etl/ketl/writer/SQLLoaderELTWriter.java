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

import java.sql.Connection;
import java.sql.SQLException;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderStatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class SQLLoaderELTWriter.
 */
public class SQLLoaderELTWriter extends BulkLoaderELTWriter {

    /** The Constant CONNECTIONSTRING_ATTRIB. */
    private static final String CONNECTIONSTRING_ATTRIB = "CONNECTIONSTRING";

    /**
     * Instantiates a new SQL loader ELT writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public SQLLoaderELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.JDBCELTWriter#prepareStatementWrapper(java.sql.Connection, java.lang.String, com.kni.etl.dbutils.JDBCItemHelper)
     */
    @Override
    StatementWrapper prepareStatementWrapper(Connection Connection, String loadStatement, JDBCItemHelper jdbcHelper)
            throws SQLException {

        boolean enableRounding = XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(),"ENABLEROUNDING",false);
        return SQLLoaderStatementWrapper.prepareStatement(Connection, this.mTargetTable, loadStatement,
                this.madcdColumns, jdbcHelper, this.pipeData(),enableRounding);
    }

    /** The OS command. */
    private String mOSCommand;
    
    /** The target table. */
    private String mTargetTable;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.JDBCELTWriter#buildInBatchSQL(java.lang.String)
     */
    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        this.mOSCommand = this.getStepTemplate(this.mDBType, "SQLLDR", true);

        boolean parallel = XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "PARALLEL", true);

        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "CONNECTIONSTRING", this
                .getParameterValue(0, SQLLoaderELTWriter.CONNECTIONSTRING_ATTRIB));
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "BINDSIZE", "BINDSIZE="
                + Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(), "BINDSIZE", 50000)));

        if (parallel)
            this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "ROWS", "");
        else
            this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "ROWS", "ROWS="
                    + Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(), "ROWS",
                            this.miCommitSize)));

        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "PARALLEL", "PARALLEL="
                + (parallel ? "TRUE" : "FALSE"));
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "DIRECT", "DIRECT="
                + (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "DIRECT", true) ? "TRUE"
                        : "FALSE"));
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "PASSWORD", this.getParameterValue(0,
                DBConnection.PASSWORD_ATTRIB));
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "USER", this.getParameterValue(0,
                DBConnection.USER_ATTRIB));
        this.mTargetTable = pTable;
        return this.mOSCommand;
    }

}
