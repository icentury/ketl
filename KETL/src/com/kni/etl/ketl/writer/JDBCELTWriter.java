/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.ketl.writer;

import java.sql.Connection;
import java.sql.SQLException;

import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.JDBCStatementWrapper;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;

/**
 * <p>
 * Title: JDBCELTWriter
 * </p>
 * <p>
 * Description: Similar functionality to JDBC writer but the data is bulk loaded into a new table in the database then
 * joined to the destination table to create a final table. This code is beta and the following items still need to be
 * addressed. 1. Extra row created - bug 2. Support for slowly changing dimensions 3. Total index recreation 4. Support
 * for DB specific bulk loader API's, such as COPY in PgSQL 5. Support for roll and archive of new and old table 6.
 * Support for partition swapping 7. Support for lookups direclty in the db 8. Support for partitioning key in temp
 * table Once this is done this approach to loading will leverage the database for greater performance
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class JDBCELTWriter extends DatabaseELTWriter {

    public JDBCELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
            throws KETLThreadException {
        super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
    }

    @Override
    StatementWrapper prepareStatementWrapper(Connection Connection, String sql, JDBCItemHelper jdbcHelper)
            throws SQLException {
        return JDBCStatementWrapper.prepareStatement(Connection, sql, jdbcHelper);
    }

    @Override
    protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException {
        if (hdl == null)
            return new JDBCItemHelper();
        else {
            try {
                Class cl = Class.forName(hdl);
                return (JDBCItemHelper) cl.newInstance();
            } catch (Exception e) {
                throw new KETLThreadException("HANDLER class not found", e, this);
            }
        }
    }

    @Override
    protected String buildInBatchSQL(String pTable) throws Exception {

        String template = this.getStepTemplate(this.mDBType, "INSERT", true);

        template = EngineConstants.replaceParameterV2(template, "DEDUPECOLUMN", this.mHandleDuplicateKeys ? ",seqcol"
                : "");
        template = EngineConstants.replaceParameterV2(template, "DESTINATIONTABLENAME", pTable);
        template = EngineConstants.replaceParameterV2(template, "DESTINATIONCOLUMNS", this.getAllColumns());
        template = EngineConstants.replaceParameterV2(template, "VALUES", this.getInsertValues());

        return template;
    }

}
