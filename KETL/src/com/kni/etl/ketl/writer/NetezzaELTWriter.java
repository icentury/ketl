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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.BulkLoaderStatementWrapper;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.dbutils.netezza.NetezzaStatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.util.XMLHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class NetezzaELTWriter.
 */
public class NetezzaELTWriter extends BulkLoaderELTWriter {

    /** The Constant HOSTNAME_ATTRIB. */
    private static final String HOSTNAME_ATTRIB = "HOSTNAME";

    /**
     * Instantiates a new netezza ELT writer.
     * 
     * @param pXMLConfig the XML config
     * @param pPartitionID the partition ID
     * @param pPartition the partition
     * @param pThreadManager the thread manager
     * 
     * @throws KETLThreadException the KETL thread exception
     */
    public NetezzaELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.JDBCELTWriter#prepareStatementWrapper(java.sql.Connection, java.lang.String, com.kni.etl.dbutils.JDBCItemHelper)
     */
    @Override
    StatementWrapper prepareStatementWrapper(Connection Connection, String loadStatement, JDBCItemHelper jdbcHelper)
            throws SQLException {

        return NetezzaStatementWrapper.prepareStatement(Connection, this.mEncoding, this.mTargetTable, loadStatement,
                this.madcdColumns, jdbcHelper, this.pipeData());
    }

    /** The OS command. */
    private String mOSCommand;
    
    /** The target table. */
    private String mTargetTable;
    
    /** The encoding. */
    private String mEncoding;
    
    /** The hostname. */
    private String mHostname;
    
    /** The DB name. */
    private String mDBName;

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.JDBCELTWriter#buildInBatchSQL(java.lang.String)
     */
    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        this.mOSCommand = this.getStepTemplate(this.mDBType, "NZLOAD", true);

        String URL = this.getParameterValue(0, DBConnection.URL_ATTRIB);
        int hostStartPos = URL.indexOf("//");
        int hostEndPos = URL.indexOf(":", hostStartPos);
        if (hostEndPos == -1)
            hostEndPos = URL.indexOf("/", hostStartPos);

        if (hostStartPos == -1 || hostEndPos == -1)
            throw new KETLThreadException("Could not determine hostname from URL", this);

        this.mHostname = URL.substring(hostStartPos + 2, hostEndPos);
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "HOSTNAME", this.mHostname);

        int dbStartPos = URL.lastIndexOf("/");
        this.mDBName = URL.substring(dbStartPos + 1);

        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "DATABASE", this.mDBName);
        this.mEncoding = XMLHelper.getAttributeAsBoolean(this.getXMLConfig().getAttributes(), "MULTIBYTE", false) ? "UTF8"
                : "LATIN9";

        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "PASSWORD", this.getParameterValue(0,
                DBConnection.PASSWORD_ATTRIB));
        this.mOSCommand = EngineConstants.replaceParameterV2(this.mOSCommand, "USER", this.getParameterValue(0,
                DBConnection.USER_ATTRIB));
        this.mTargetTable = pTable;
        return this.mOSCommand;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ketl.writer.DatabaseELTWriter#close(boolean)
     */
    @Override
    protected void close(boolean success) {
        if (success == false && this.isLastThreadToEnterCompletePhase()
                && ((BulkLoaderStatementWrapper) this.stmt).loaderExecuted()) {
            // run nzreclaim
            try {
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                        "nzreclaim will be executed to cleanup the target table");
                ((NetezzaStatementWrapper) this.stmt).reclaim(this.getParameterValue(0, DBConnection.USER_ATTRIB), this
                        .getParameterValue(0, DBConnection.PASSWORD_ATTRIB), this.mTargetTable, this.mDBName,
                        this.mHostname);
            } catch (IOException e) {
                ResourcePool.LogException(e, Thread.currentThread());
                e.printStackTrace();
            }
        }
        super.close(success);
    }

}
