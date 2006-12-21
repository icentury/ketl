/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.BulkLoaderStatementWrapper;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.dbutils.netezza.NetezzaStatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

public class NetezzaELTWriter extends BulkLoaderELTWriter {

    private static final String HOSTNAME_ATTRIB = "HOSTNAME";

    public NetezzaELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    StatementWrapper prepareStatementWrapper(Connection Connection, String loadStatement, JDBCItemHelper jdbcHelper)
            throws SQLException {

        return NetezzaStatementWrapper.prepareStatement(Connection, mEncoding, mTargetTable, loadStatement,
                madcdColumns, jdbcHelper, this.pipeData());
    }

    private String mOSCommand;
    private String mTargetTable;
    private String mEncoding;
    private String mHostname;
    private String mDBName;

    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        mOSCommand = this.getStepTemplate(mDBType, "NZLOAD", true);

        String URL = this.getParameterValue(0, URL_ATTRIB);
        int hostStartPos = URL.indexOf("//");
        int hostEndPos = URL.indexOf(":", hostStartPos);
        if (hostEndPos == -1)
            hostEndPos = URL.indexOf("/", hostStartPos);

        if (hostStartPos == -1 || hostEndPos == -1)
            throw new KETLThreadException("Could not determine hostname from URL", this);

        mHostname = URL.substring(hostStartPos + 2, hostEndPos);
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "HOSTNAME", mHostname);

        int dbStartPos = URL.lastIndexOf("/");
        mDBName = URL.substring(dbStartPos + 1);

        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "DATABASE", mDBName);
        mEncoding = XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "MULTIBYTE", false) ? "UTF8"
                : "LATIN9";

        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "PASSWORD", this.getParameterValue(0,
                PASSWORD_ATTRIB));
        mOSCommand = EngineConstants.replaceParameterV2(mOSCommand, "USER", this.getParameterValue(0, USER_ATTRIB));
        mTargetTable = pTable;
        return mOSCommand;
    }

    @Override
    protected void close(boolean success) {
        if (success == false && this.isLastThreadToEnterCompletePhase()
                && ((BulkLoaderStatementWrapper) this.stmt).loaderExecuted()) {
            // run nzreclaim
            try {
                ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.ERROR_MESSAGE, "nzreclaim will be executed to cleanup the target table");
                ((NetezzaStatementWrapper) this.stmt).reclaim(this.getParameterValue(0, USER_ATTRIB), this
                        .getParameterValue(0, PASSWORD_ATTRIB), mTargetTable, mDBName, mHostname);
            } catch (IOException e) {
                ResourcePool.LogException(e, Thread.currentThread());
                e.printStackTrace();
            }
        }
        super.close(success);
    }

}
