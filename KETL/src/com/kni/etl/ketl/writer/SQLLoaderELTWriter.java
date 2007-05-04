/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
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

public class SQLLoaderELTWriter extends BulkLoaderELTWriter {

    private static final String CONNECTIONSTRING_ATTRIB = "CONNECTIONSTRING";

    public SQLLoaderELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    StatementWrapper prepareStatementWrapper(Connection Connection, String loadStatement, JDBCItemHelper jdbcHelper)
            throws SQLException {

        return SQLLoaderStatementWrapper.prepareStatement(Connection, this.mTargetTable, loadStatement,
                this.madcdColumns, jdbcHelper, this.pipeData());
    }

    private String mOSCommand;
    private String mTargetTable;

    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        this.mOSCommand = this.getStepTemplate(this.mDBType, "SQLLDR", true);

        boolean parallel = XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "PARALLEL", true);

        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "CONNECTIONSTRING", this
                .getParameterValue(0, SQLLoaderELTWriter.CONNECTIONSTRING_ATTRIB));
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "BINDSIZE", "BINDSIZE="
                + Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(), "BINDSIZE", 1000)));

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
