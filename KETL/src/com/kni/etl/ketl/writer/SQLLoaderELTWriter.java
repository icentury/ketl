/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.sql.Connection;
import java.sql.SQLException;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderItemHelper;
import com.kni.etl.ketl.dbutils.oracle.SQLLoaderStatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
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

        return SQLLoaderStatementWrapper.prepareStatement(Connection, mTargetTable, loadStatement, madcdColumns,
                jdbcHelper, this.pipeData());
    }

    private String mOSCommand;
    private String mTargetTable;

    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        mOSCommand = this.getStepTemplate(mDBType, "SQLLDR", true);

        boolean parallel = XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "PARALLEL", true);

        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "CONNECTIONSTRING", this.getParameterValue(0,
                CONNECTIONSTRING_ATTRIB));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "BINDSIZE", "BINDSIZE="
                + Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(), "BINDSIZE", 1000)));

        if (parallel)
            mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "ROWS", "");
        else
            mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "ROWS", "ROWS="
                    + Integer.toString(XMLHelper.getAttributeAsInt(this.getXMLConfig().getAttributes(), "ROWS",
                            this.miCommitSize)));

        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "PARALLEL", "PARALLEL="
                + (parallel ? "TRUE" : "FALSE"));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "DIRECT", "DIRECT="
                + (XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "DIRECT", true) ? "TRUE"
                        : "FALSE"));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "PASSWORD", this.getParameterValue(0,
                PASSWORD_ATTRIB));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "USER", this.getParameterValue(0, USER_ATTRIB));
        mTargetTable = pTable;
        return mOSCommand;
    }

}
