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
import com.kni.etl.dbutils.JDBCStatementWrapper;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.smp.ETLThreadManager;

// TODO: Auto-generated Javadoc
/**
 * <p>
 * Title: JDBCELTWriter
 * </p>
 * <p>
 * Description: Similar functionality to JDBC writer but the data is bulk loaded
 * into a new table in the database then joined to the destination table to
 * create a final table. This code is beta and the following items still need to
 * be addressed. 1. Extra row created - bug 2. Support for slowly changing
 * dimensions 3. Total index recreation 4. Support for DB specific bulk loader
 * API's, such as COPY in PgSQL 5. Support for roll and archive of new and old
 * table 6. Support for partition swapping 7. Support for lookups direclty in
 * the db 8. Support for partitioning key in temp table Once this is done this
 * approach to loading will leverage the database for greater performance
 * </p>
 * 
 * @author Brian Sullivan
 * @version 0.1
 */
public class JDBCELTWriter extends DatabaseELTWriter {

	@Override
	protected String getVersion() {
		return "$LastChangedRevision$";
	}

	/**
	 * Instantiates a new JDBCELT writer.
	 * 
	 * @param pXMLConfig
	 *            the XML config
	 * @param pPartitionID
	 *            the partition ID
	 * @param pPartition
	 *            the partition
	 * @param pThreadManager
	 *            the thread manager
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public JDBCELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager) throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.writer.DatabaseELTWriter#prepareStatementWrapper(java
	 * .sql.Connection, java.lang.String, com.kni.etl.dbutils.JDBCItemHelper)
	 */
	@Override
	StatementWrapper prepareStatementWrapper(Connection Connection, String sql, JDBCItemHelper jdbcHelper) throws SQLException {
		return JDBCStatementWrapper.prepareStatement(Connection, sql, jdbcHelper);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.writer.DatabaseELTWriter#instantiateHelper(java.lang
	 * .String)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.kni.etl.ketl.writer.DatabaseELTWriter#buildInBatchSQL(java.lang.String
	 * )
	 */
	@Override
	protected String buildInBatchSQL(String pTable) throws Exception {

		String template = this.getStepTemplate(this.getGroup(), "INSERT", true);

		template = EngineConstants.replaceParameterV2(template, "DEDUPECOLUMN", this.mHandleDuplicateKeys ? ",seqcol" : "");
		template = EngineConstants.replaceParameterV2(template, "DESTINATIONTABLENAME", pTable);
		template = EngineConstants.replaceParameterV2(template, "DESTINATIONCOLUMNS", this.getAllColumns());
		template = EngineConstants.replaceParameterV2(template, "VALUES", this.getInsertValues());

		return template;
	}

}
