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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.kni.etl.EngineConstants;
import com.kni.etl.dbutils.DatabaseColumnDefinition;
import com.kni.etl.dbutils.JDBCItemHelper;
import com.kni.etl.dbutils.PrePostSQL;
import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.dbutils.StatementManager;
import com.kni.etl.dbutils.StatementWrapper;
import com.kni.etl.ketl.DBConnection;
import com.kni.etl.ketl.ETLInPort;
import com.kni.etl.ketl.ETLStep;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLThreadException;
import com.kni.etl.ketl.exceptions.KETLWriteException;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.DefaultWriterCore;
import com.kni.etl.ketl.smp.ETLThreadManager;
import com.kni.etl.ketl.smp.WriterBatchManager;
import com.kni.etl.util.XMLHelper;

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
abstract public class DatabaseELTWriter extends ETLWriter implements DefaultWriterCore, DBConnection,
		WriterBatchManager, PrePostSQL {

	/**
	 * Instantiates a new database ELT writer.
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
	public DatabaseELTWriter(Node pXMLConfig, int pPartitionID, int pPartition, ETLThreadManager pThreadManager)
			throws KETLThreadException {
		super(pXMLConfig, pPartitionID, pPartition, pThreadManager);

	}

	/**
	 * The Class Index.
	 */
	class Index {

		/** The columns. */
		ArrayList mColumns = new ArrayList();

		/** The name. */
		String mName;

		/** The non unique. */
		boolean mNonUnique;
	}

	/**
	 * The Class IndexColumn.
	 */
	class IndexColumn {

		/** The ascending. */
		boolean mAscending;

		/** The column. */
		String mColumn;

		/** The position. */
		short mPosition;
	}

	/** The Constant ALTERNATE_INSERT_VALUE. */
	public static final String ALTERNATE_INSERT_VALUE = "ALTERNATE_INSERT_VALUE";

	/** The Constant ALTERNATE_UPDATE_VALUE. */
	public static final String ALTERNATE_UPDATE_VALUE = "ALTERNATE_UPDATE_VALUE";

	/** The Constant ALTERNATE_VALUE_SUB. */
	private static final String ALTERNATE_VALUE_SUB = "${PARAM}";

	/** The Constant BATCH_ATTRIB. */
	public static final String BATCH_ATTRIB = "BATCHDATA";

	/** The Constant HANDLER_ATTRIB. */
	public static final String HANDLER_ATTRIB = "HANDLER";

	/** The Constant WRAP_ATTRIB. */
	public static final String WRAP_ATTRIB = "WRAP";

	/** The Constant HASH_COLUMN_ATTRIB. */
	public static final String HASH_COLUMN_ATTRIB = "HASHCOLUMN";

	/** The Constant HASH_COMPARE_ONLY_ATTRIB. */
	public static final String HASH_COMPARE_ONLY_ATTRIB = "HASHCOMPAREONLY";

	/** The Constant BULK. */
	private static final int BULK = 2;

	/** The Constant COMMITSIZE_ATTRIB. */
	public static final String COMMITSIZE_ATTRIB = "COMMITSIZE";

	/** The Constant COMPARE_ATTRIB. */
	public static final String COMPARE_ATTRIB = "COMPARE";

	/** The Constant INSERT_ATTRIB. */
	public static final String INSERT_ATTRIB = "INSERT";

	/** The Constant MAXTRANSACTIONSIZE_ATTRIB. */
	public static final String MAXTRANSACTIONSIZE_ATTRIB = "MAXTRANSACTIONSIZE";

	/** The Constant LOWER_CASE. */
	static final int LOWER_CASE = 0;

	/** The Constant MIXED_CASE. */
	static final int MIXED_CASE = 2;

	/** The Constant PK_ATTRIB. */
	public static final String PK_ATTRIB = "PK";

	/** The Constant ROLL. */
	private static final int ROLL = 0;

	/** The Constant SCHEMA_ATTRIB. */
	public static final String SCHEMA_ATTRIB = "SCHEMA";

	/** The Constant SEQUENCE_ATTRIB. */
	public static final String SEQUENCE_ATTRIB = "SEQUENCE";

	/** The Constant SK_ATTRIB. */
	public static final String SK_ATTRIB = "SK";

	/** The Constant SWAP_PARTITION. */
	static final int SWAP_PARTITION = 0;

	/** The Constant SWAP_TABLE. */
	static final int SWAP_TABLE = 1;

	/** The Constant TABLE_ATTRIB. */
	public static final String TABLE_ATTRIB = "TABLE";

	/** The Constant TYPE_ATTRIB. */
	public static final String TYPE_ATTRIB = "TYPE";

	/** The Constant STREAM_ATTRIB. */
	public static final String STREAM_ATTRIB = "STREAMCHANGES";

	/** The Constant UPDATE_ATTRIB. */
	public static final String UPDATE_ATTRIB = "UPDATE";

	/** The Constant IGNOREINVALIDCOLUMNS_ATTRIB. */
	public static final String IGNOREINVALIDCOLUMNS_ATTRIB = "IGNOREINVALIDCOLUMNS";

	/** The Constant UPPER_CASE. */
	static final int UPPER_CASE = 1;

	/** The Constant UPSERT. */
	private static final int UPSERT = 1;

	/** The Constant HANDLE_DUPLICATES_ATTRIB. */
	private static final String HANDLE_DUPLICATES_ATTRIB = "HANDLEDUPLICATES";

	/** The madcd columns. */
	DatabaseColumnDefinition[] madcdColumns = null;

	/** The ma other columns. */
	String[] maOtherColumns = null;

	/** The batch data. */
	boolean mBatchData = true;

	/** The stream changes. */
	boolean mStreamChanges = true;

	/** The mc DB connection. */
	protected Connection mcDBConnection;

	/** The DB case. */
	int mDBCase = -1;

	/** The DB type. */
	String mDBType = null;

	/** The dont compound statements. */
	boolean mDontCompoundStatements = false;

	/** The mi commit size. */
	int miCommitSize;

	/** The mi field population order. */
	private int[] miFieldPopulationOrder;

	/** The mi insert count. */
	int miInsertCount = 0;

	/** The mi max transaction size. */
	int miMaxTransactionSize = -1;

	/** The mi replace technique. */
	int miReplaceTechnique = DatabaseELTWriter.SWAP_TABLE;

	/** The primary key specified. */
	private boolean mPrimaryKeySpecified = false;

	/** The ms all columns. */
	private String msAllColumns;

	/** The ms in batch SQL statement. */
	private String msInBatchSQLStatement = null;

	/** The ms insert values. */
	private String msInsertValues;

	/** The ms join. */
	String msJoin;

	/** The source key specified. */
	private boolean mSourceKeySpecified = false;

	/** The ms post batch SQL. */
	private Object[] msPostBatchSQL = null;

	/** The ms post load SQL. */
	private Object[] msPostLoadSQL = null;

	/** The ms pre load SQL. */
	private Object[] msPreLoadSQL = null;

	/** The ms join columns. */
	private String msJoinColumns;

	/** The jdbc helper. */
	private JDBCItemHelper jdbcHelper;

	/** The ms temp table name. */
	String msTempTableName = null;

	/** The mstr schema name. */
	String mstrSchemaName = null;

	/** The mstr table name. */
	String mstrTableName = null;

	/** The ms update columns. */
	String msUpdateColumns;

	/** The ms update triggers. */
	String msUpdateTriggers;

	/** The type. */
	int mType = -1;

	/** The used connections. */
	ArrayList mUsedConnections = new ArrayList();

	/** The mv column index. */
	HashMap mvColumnIndex = new HashMap();

	/** The mv columns. */
	Vector mvColumns = new Vector(); // for building the column list and
										// later converting it into the array

	/** The str driver class. */
	private String strDriverClass = null;

	/** The str password. */
	private String strPassword = null;

	/** The str pre SQL. */
	private String strPreSQL = null;

	/** The str URL. */
	private String strURL = null;

	/** The str user name. */
	private String strUserName = null;

	/** The ms insert source columns. */
	private String msInsertSourceColumns;

	/** The mi analyze pos. */
	private int miAnalyzePos = -1;

	/**
	 * The Class JDBCETLInPort.
	 */
	public class JDBCETLInPort extends ETLInPort {

		/**
		 * The Class JDBCDatabaseColumnDefinition.
		 */
		class JDBCDatabaseColumnDefinition extends DatabaseColumnDefinition {

			/*
			 * (non-Javadoc)
			 * 
			 * @see com.kni.etl.dbutils.DatabaseColumnDefinition#getSourceClass()
			 */
			@Override
			public Class getSourceClass() {
				return JDBCETLInPort.this.getPortClass();
			}

			/**
			 * Instantiates a new JDBC database column definition.
			 * 
			 * @param pNode
			 *            the node
			 * @param pColumnName
			 *            the column name
			 * @param pDataType
			 *            the data type
			 */
			public JDBCDatabaseColumnDefinition(Node pNode, String pColumnName, int pDataType) {
				super(pNode, pColumnName, pDataType);
			}

		}

		/** The dcd new column. */
		DatabaseColumnDefinition dcdNewColumn;

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.kni.etl.ketl.ETLInPort#initialize(org.w3c.dom.Node)
		 */
		@Override
		public int initialize(Node xmlNode) throws ClassNotFoundException, KETLThreadException {

			super.initialize(xmlNode);

			// Create a new column definition with the default properties...
			this.dcdNewColumn = new JDBCDatabaseColumnDefinition(xmlNode, "", 0);

			NamedNodeMap attr = xmlNode.getAttributes();
			// Get the column's target name...
			this.dcdNewColumn.setColumnName(this.getPortName());

			this.dcdNewColumn.setAlternateInsertValue(XMLHelper.getAttributeAsString(attr,
					DatabaseELTWriter.ALTERNATE_INSERT_VALUE, null));
			this.dcdNewColumn.setAlternateUpdateValue(XMLHelper.getAttributeAsString(attr,
					DatabaseELTWriter.ALTERNATE_UPDATE_VALUE, null));
			this.dcdNewColumn
					.setValueWrapper(XMLHelper.getAttributeAsString(attr, DatabaseELTWriter.WRAP_ATTRIB, null));

			// Find out what the upsert flags are for this input...
			if (XMLHelper.getAttributeAsBoolean(attr, DatabaseELTWriter.PK_ATTRIB, false)) {
				this.dcdNewColumn.setProperty(DatabaseColumnDefinition.PRIMARY_KEY);
				DatabaseELTWriter.this.mPrimaryKeySpecified = true;
			}

			// Source key
			if (XMLHelper.getAttributeAsBoolean(attr, DatabaseELTWriter.SK_ATTRIB, false)) {
				this.dcdNewColumn.setProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY);
				DatabaseELTWriter.this.mSourceKeySpecified = true;
			}

			// Insert field
			if (XMLHelper.getAttributeAsBoolean(attr, DatabaseELTWriter.INSERT_ATTRIB, true)) {
				this.dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN);
			}

			// Update field
			if (XMLHelper.getAttributeAsBoolean(attr, DatabaseELTWriter.UPDATE_ATTRIB, true)) {
				this.dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_COLUMN);
			}

			// Compare field, drives updates
			if (XMLHelper.getAttributeAsBoolean(attr, DatabaseELTWriter.COMPARE_ATTRIB, true)) {
				this.dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN);
			}

			this.dcdNewColumn.exists = false;
			// It's ok if not specified
			DatabaseELTWriter.this.mvColumns.add(this.dcdNewColumn);
			DatabaseELTWriter.this.mvColumnIndex.put(this.dcdNewColumn.getColumnName(null,
					DatabaseELTWriter.this.mDBCase), this.dcdNewColumn);

			return 0;
		}

		/**
		 * Instantiates a new JDBCETL in port.
		 * 
		 * @param esOwningStep
		 *            the es owning step
		 * @param esSrcStep
		 *            the es src step
		 */
		public JDBCETLInPort(ETLStep esOwningStep, ETLStep esSrcStep) {
			super(esOwningStep, esSrcStep);
		}

	}

	/**
	 * Define hash column.
	 * 
	 * @param name
	 *            the name
	 * 
	 * @return the database column definition
	 */
	private DatabaseColumnDefinition defineHashColumn(String name) {
		DatabaseColumnDefinition dcdNewColumn = new DatabaseColumnDefinition(null, name, java.sql.Types.INTEGER);

		dcdNewColumn.setProperty(DatabaseColumnDefinition.HASH_COLUMN);
		dcdNewColumn.setProperty(DatabaseColumnDefinition.INSERT_COLUMN);
		dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_COLUMN);
		dcdNewColumn.setProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN);

		dcdNewColumn.exists = false;

		this.mvColumns.add(0, dcdNewColumn);
		this.mvColumnIndex.put(dcdNewColumn.getColumnName(null, this.mDBCase), dcdNewColumn);

		return dcdNewColumn;
	}

	/**
	 * Builds the in batch SQL.
	 * 
	 * @param pTable
	 *            the table
	 * 
	 * @return the string
	 * 
	 * @throws Exception
	 *             the exception
	 */
	abstract protected String buildInBatchSQL(String pTable) throws Exception;

	/**
	 * Builds the post batch SQL.
	 * 
	 * @return the object[]
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private Object[] buildPostBatchSQL() throws Exception {

		// UPDATE TABLE OR DELETE ROWS
		// INSERT NEW ROWS OR DELETE ROWS
		ArrayList sql = new ArrayList();

		if (this.mType == DatabaseELTWriter.UPSERT) {
			if (this.mStreamChanges)
				this.getUpsertSQL(sql);
		}
		return sql.toArray();

	}

	/**
	 * Gets the upsert SQL.
	 * 
	 * @param sql
	 *            the sql
	 * 
	 * @return the upsert SQL
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 * @throws SQLException
	 *             the SQL exception
	 */
	private void getUpsertSQL(ArrayList sql) throws KETLThreadException, SQLException {

		if (this.mStreamChanges == false) {
			String template = this.getStepTemplate(this.mDBType, "CREATEINDEX", true);

			template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrSchemaName
					+ this.msTempTableName);
			template = EngineConstants.replaceParameterV2(template, "INDEXNAME", this.getUniqueObjectName("idx"));
			template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.msJoinColumns);

			sql.add(template);
		}

		String sqlToExecute = this.getStepTemplate(this.mDBType, "ANALYZETABLE", true);
		sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mstrSchemaName
				+ this.msTempTableName);
		sql.add(sqlToExecute);

		this.miAnalyzePos = sql.indexOf(sqlToExecute);

		if (this.mHandleDuplicateKeys) {
			sqlToExecute = this.getStepTemplate(this.mDBType, "DELETEDUPLICATES", true);
			sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
			sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME", "aSub");
			sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
					+ this.msTempTableName);
			sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "ALIASNAME", this.mstrTableName);
			sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DEDUPECOLUMN", "seqcol");
			sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "KEYS", this.msJoinColumns);
			sql.add(sqlToExecute);

		}

		if (this.mbReplaceMode) {
			// delete from target rows coming in

			sqlToExecute = this.getStepTemplate(this.mDBType, "DELETESTATICROWS", true);
			if (sqlToExecute != null && sqlToExecute.equals("") == false) {
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NAMEDSOURCECOLUMNS",
						this.msTempTableName + "."
								+ this.getAllColumns().replace(",", "," + this.msTempTableName + "."));
				sqlToExecute = EngineConstants
						.replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
						+ this.msTempTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "FIRSTSK", this.mFirstSK);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "KEYS", this.msJoinColumns);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCEKEYS", this.msTempTableName
						+ "." + this.msJoinColumns.replace(",", "," + this.msTempTableName + "."));
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
						this.mstrSchemaName + this.mstrTableName);
				sql.add(sqlToExecute);
			}

			sqlToExecute = this.getStepTemplate(this.mDBType, "DELETETARGET", true);
			if (sqlToExecute != null && sqlToExecute.equals("") == false) {
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
						+ this.msTempTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
						this.mstrSchemaName + this.mstrTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "KEYS", this.msJoinColumns);
				sql.add(sqlToExecute);
			}

			sqlToExecute = this.getStepTemplate(this.mDBType, "INSERTTARGET", true);
			if (sqlToExecute != null && sqlToExecute.equals("") == false) {
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCECOLUMNS", this.getAllColumns());
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTCOLUMNS",
						this.msInsertSourceColumns);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTDESTINATIONCOLUMNS",
						this.msInsertColumns);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
						+ this.msTempTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
						this.mstrSchemaName + this.mstrTableName);
				sql.add(sqlToExecute);
			}

		} else {
			sqlToExecute = this.getStepTemplate(this.mDBType, "UPDATEPOSTBATCH", true);
			if (sqlToExecute != null && sqlToExecute.equals("") == false) {
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
				sqlToExecute = EngineConstants
						.replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
						+ this.msTempTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
						this.mstrSchemaName + this.mstrTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
				sql.add(sqlToExecute);
			}

			sqlToExecute = this.getStepTemplate(this.mDBType, "INSERTPOSTBATCH", true);
			if (sqlToExecute != null && sqlToExecute.equals("") == false) {
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "JOIN", this.msJoin);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCECOLUMNS", this.getAllColumns());
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "BESTJOIN", this.msBestJoin);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "NAMEDSOURCECOLUMNS",
						this.msTempTableName + "."
								+ this.getAllColumns().replace(",", "," + this.msTempTableName + "."));
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTCOLUMNS",
						this.msInsertSourceColumns);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "UPDATECOLUMNS", this.msUpdateColumns);
				sqlToExecute = EngineConstants
						.replaceParameterV2(sqlToExecute, "UPDATETRIGGERS", this.msUpdateTriggers);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "INSERTDESTINATIONCOLUMNS",
						this.msInsertColumns);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "SOURCETABLENAME", this.mstrSchemaName
						+ this.msTempTableName);
				sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "DESTINATIONTABLENAME",
						this.mstrSchemaName + this.mstrTableName);
				sql.add(sqlToExecute);
			}
		}
		sqlToExecute = this.getStepTemplate(this.mDBType, "TRUNCATETABLE", true);
		sqlToExecute = EngineConstants.replaceParameterV2(sqlToExecute, "TABLENAME", this.mstrSchemaName
				+ this.msTempTableName);
		sql.add(sqlToExecute);
	}

	/** The index enable list. */
	private ArrayList mIndexEnableList = new ArrayList();

	/** The index disable list. */
	private ArrayList mIndexDisableList = new ArrayList();

	/**
	 * Builds the post load SQL.
	 * 
	 * @return the object[]
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private Object[] buildPostLoadSQL() throws Exception {

		ArrayList sql = new ArrayList();
		try {

			switch (this.mType) {

			case ROLL:
				String sh = this.setDBCase(XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(),
						DatabaseELTWriter.SCHEMA_ATTRIB, null));

				ResultSet rs = this.mcDBConnection.getMetaData()
						.getIndexInfo(null, sh, this.mstrTableName, false, true);
				HashMap hm = new HashMap();
				while (rs.next()) {
					String idxName = rs.getString(6);
					if (idxName == null)
						idxName = "NULL";
					Index idx = (Index) hm.get(idxName);
					if (idx == null) {
						idx = new Index();
						idx.mName = idxName;
						idx.mNonUnique = rs.getBoolean(4);
						hm.put(idxName, idx);
					}

					IndexColumn col = new IndexColumn();
					if (rs.getString(10) != null && rs.getString(10).equalsIgnoreCase("D"))
						col.mAscending = false;
					col.mColumn = rs.getString(9);
					col.mPosition = rs.getShort(8);
					idx.mColumns.add(col);
				}

				String tableName = this.getUniqueObjectName(this.mstrTableName);
				String indexName = this.getUniqueObjectName("i");
				StringBuffer sb = new StringBuffer();
				StringBuffer si = new StringBuffer();
				StringBuffer ssk = new StringBuffer();
				StringBuffer spk = new StringBuffer();
				StringBuffer addPK = new StringBuffer();

				for (DatabaseColumnDefinition element : this.madcdColumns) {
					if (sb.length() > 0) {
						sb.append(",\n\t");
					}

					String prim = "a.", sec = "b.";
					if (element.hasProperty(DatabaseColumnDefinition.PRIMARY_KEY)
							|| element.hasProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY)) {
						prim = "b.";
						sec = "a.";
					}
					sb.append(this.coalesce((element.getAlternateUpdateValue() == null ? prim
							+ element.getColumnName(this.getIDQuote(), this.mDBCase) : element
							.getAlternateUpdateValue().replace(DatabaseELTWriter.ALTERNATE_VALUE_SUB,
									prim + element.getColumnName(this.getIDQuote(), this.mDBCase))), (element
							.getAlternateInsertValue() == null ? sec
							+ element.getColumnName(this.getIDQuote(), this.mDBCase) : element
							.getAlternateInsertValue().replace(DatabaseELTWriter.ALTERNATE_VALUE_SUB,
									sec + element.getColumnName(this.getIDQuote(), this.mDBCase))))
							+ " AS " + element.getColumnName(this.getIDQuote(), this.mDBCase));

				}

				String keyColumns = sb.toString();

				sb = new StringBuffer();

				for (int i = 0; i < this.maOtherColumns.length; i++) {
					if (i > 0) {
						sb.append(",\n\t");
					}

					sb.append("b." + this.maOtherColumns[i]);
				}

				String otherColumns = sb.toString();

				if (keyColumns.length() > 0 && otherColumns.length() > 0)
					otherColumns = "," + otherColumns;

				sb = new StringBuffer();

				for (DatabaseColumnDefinition element : this.madcdColumns) {
					if (element.hasProperty(DatabaseColumnDefinition.SRC_UNIQUE_KEY)) {
						if (sb.length() > 0) {
							sb.append(" AND \n\t");
							si.append(',');
							ssk.append(',');
							addPK.append(',');
						}

						sb.append("a." + element.getColumnName(this.getIDQuote(), this.mDBCase) + " = b."
								+ element.getColumnName(this.getIDQuote(), this.mDBCase));
						si.append(element.getColumnName(this.getIDQuote(), this.mDBCase));
						ssk.append(element.getColumnName(this.getIDQuote(), this.mDBCase));
						addPK.append(element.getColumnName(this.getIDQuote(), this.mDBCase));
					}
				}

				// CREATE TABLE ${NEWTABLENAME} AS SELECT ${KEYCOLUMNS}
				// ${OTHERCOLUMNS} FROM ${STGTABLE} as a full outer
				// join ${ORIGTABLE} as b on (${JOIN})
				String rollSQL = this.getStepTemplate(this.mDBType, "MERGETONEWTABLE", true);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "NEWTABLENAME", this.mstrSchemaName + tableName);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "KEYCOLUMNS", keyColumns);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "OTHERCOLUMNS", otherColumns);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "STGTABLE", this.mstrSchemaName
						+ this.msTempTableName);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "ORIGTABLE", this.mstrSchemaName
						+ this.mstrTableName);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "JOIN", sb.toString());

				for (DatabaseColumnDefinition element : this.madcdColumns) {
					if (element.hasProperty(DatabaseColumnDefinition.PRIMARY_KEY)) {
						if (spk.length() > 0) {
							spk.append(',');
						}

						spk.append(element.getColumnName(this.getIDQuote(), this.mDBCase));
					}
				}

				// add source unique key end to source table
				sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(EngineConstants
						.replaceParameterV2(this.getStepTemplate(this.mDBType, "CREATEUNIQUEINDEX", true), "INDEXNAME",
								indexName), "TABLENAME", this.mstrSchemaName + this.msTempTableName), "COLUMNS", si
						.toString()));
				sql.add(rollSQL);
				sql.add(EngineConstants.replaceParameterV2(this.getStepTemplate(this.mDBType, "DROPTABLE", true),
						"TABLENAME", this.mstrSchemaName + this.msTempTableName));
				// source primary key index
				if (this.mPrimaryKeySpecified) {
					sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(EngineConstants
							.replaceParameterV2(this.getStepTemplate(this.mDBType, "CREATEUNIQUEINDEX", true),
									"INDEXNAME", this.getUniqueObjectName("PK_" + tableName)), "TABLENAME",
							this.mstrSchemaName + tableName), "COLUMNS", spk.toString()));
					// add primary key
					sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(this.getStepTemplate(
							this.mDBType, "ADDPRIMARYKEY", true), "TABLENAME", this.mstrSchemaName + tableName),
							"COLUMNS", addPK.toString()));
				}
				if (this.mSourceKeySpecified) {
					// source uniqe key
					sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(EngineConstants
							.replaceParameterV2(this.getStepTemplate(this.mDBType, "CREATEUNIQUEINDEX", true),
									"INDEXNAME", this.getUniqueObjectName("SK_" + tableName)), "TABLENAME",
							this.mstrSchemaName + tableName), "COLUMNS", ssk.toString()));
				}

				switch (this.miReplaceTechnique) {
				case SWAP_PARTITION:
					;

				case SWAP_TABLE:
					String template = this.getStepTemplate(this.mDBType, "RENAMETABLE", true);
					sql.add(StatementManager.COMMIT);
					sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(template,
							"TABLENAME", this.mstrSchemaName + this.mstrTableName), "NEWTABLENAME", this
							.getUniqueObjectName("prev_" + this.mstrTableName)));
					sql.add(StatementManager.COMMIT);
					sql.add(EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(template,
							"TABLENAME", this.mstrSchemaName + tableName), "NEWTABLENAME", this.mstrTableName));
				}
				break;
			case UPSERT:
				if (this.mStreamChanges == false)
					this.getUpsertSQL(sql);
				rollSQL = this.getStepTemplate(this.mDBType, "DROPTABLE", true);
				rollSQL = EngineConstants.replaceParameterV2(rollSQL, "TABLENAME", this.mstrSchemaName
						+ this.msTempTableName);

				sql.add(rollSQL);
				break;
			}

			return sql.toArray();
		} catch (SQLException e) {
			ResourcePool.LogException(e, this);

			return null;
		}
	}

	/**
	 * Gets the failure cleanup load SQL.
	 * 
	 * @return the failure cleanup load SQL
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private Object[] getFailureCleanupLoadSQL() throws Exception {

		ArrayList sql = new ArrayList();

		switch (this.mType) {
		case ROLL:
		case UPSERT:
			sql.add(EngineConstants.replaceParameterV2(this.getStepTemplate(this.mDBType, "DROPTABLE", true),
					"TABLENAME", this.mstrSchemaName + this.msTempTableName));
			break;
		}

		return sql.toArray();

	}

	/**
	 * Builds the pre load SQL.
	 * 
	 * @return the object[]
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private Object[] buildPreLoadSQL() throws Exception {

		ArrayList sql = new ArrayList();

		if (this.mType == DatabaseELTWriter.UPSERT) {

			String template = this.getStepTemplate(this.mDBType, "CREATETABLE", true);

			template = EngineConstants.replaceParameterV2(template, "NEWTABLENAME", this.mstrSchemaName
					+ this.msTempTableName);
			template = EngineConstants.replaceParameterV2(template, "SOURCETABLENAME", this.mstrSchemaName
					+ this.mstrTableName);
			template = EngineConstants.replaceParameterV2(template, "SOURCECOLUMNS", this.getAllColumns());

			template = EngineConstants.replaceParameterV2(template, "DEDUPECOLUMN",
					this.mHandleDuplicateKeys ? ",1 as seqcol" : "");

			sql.add(template);

			if (this.mStreamChanges) {
				template = this.getStepTemplate(this.mDBType, "CREATEINDEX", true);

				template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrSchemaName
						+ this.msTempTableName);
				template = EngineConstants.replaceParameterV2(template, "INDEXNAME", this.getUniqueObjectName("idx"));
				template = EngineConstants.replaceParameterV2(template, "COLUMNS", this.msJoinColumns);

				sql.add(template);
			}
		}

		return sql.toArray();
	}

	/**
	 * Coalesce.
	 * 
	 * @param arg1
	 *            the arg1
	 * @param arg2
	 *            the arg2
	 * 
	 * @return the string
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private String coalesce(String arg1, String arg2) throws Exception {
		return EngineConstants.replaceParameterV2(EngineConstants.replaceParameterV2(this.getStepTemplate(this.mDBType,
				"COALESCE", true), "ARG1", arg1), "ARG2", arg2);
	}

	/** The stmt. */
	StatementWrapper stmt;

	/** The max char length. */
	private int maxCharLength;

	/** The batch counter. */
	protected int mBatchCounter;

	/** The fire pre batch. */
	protected boolean firePreBatch;

	/** The mb reinit on error. */
	private boolean mbReinitOnError;

	/**
	 * DOCUMENT ME!.
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public int complete() throws KETLThreadException {
		int res = super.complete();

		try {
			this.stmt.close();
			this.stmt = null;
		} catch (Exception e) {
			ResourcePool.LogException(e, this);
		}

		if (res < 0) {
			ResourcePool
					.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Error during final batch, see previous messages");
		} else {
			try {
				this.executePostStatements();

				if (this.mIncrementalCommit == false) {
					this.mcDBConnection.commit();
				}
			} catch (Exception e) {
				try {
					if (this.debug() == false)
						StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
								this.mStatementSeperator, StatementManager.END, this, true);
					else {
						ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE,
								"Cleanup statements were skipped, manual deletion of temp tables may be necessary");
					}
				} catch (Exception e1) {

				}
				throw new KETLThreadException("Running post load " + e.getMessage(), e, this);
			}
		}

		if (this.mcDBConnection != null) {
			ResourcePool.releaseConnection(this.mcDBConnection);
			this.mcDBConnection = null;
		}

		return res;
	}

	/**
	 * Gets the all other table columns.
	 * 
	 * @return the all other table columns
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	private String[] getAllOtherTableColumns() throws SQLException {
		Statement stmt = this.mcDBConnection.createStatement();

		ResultSet rs = stmt.executeQuery("SELECT * FROM " + this.mstrSchemaName + this.mstrTableName + " WHERE 1 = 0");

		ResultSetMetaData md = rs.getMetaData();

		ArrayList ar = new ArrayList();

		for (int i = 1; i <= md.getColumnCount(); i++) {
			String col = this.setDBCase(md.getColumnName(i));

			if (this.mvColumnIndex.containsKey(col) == false) {
				ar.add(col);
			}
		}

		String[] res = new String[ar.size()];

		ar.toArray(res);

		rs.close();
		stmt.close();

		return res;
	}

	/**
	 * Gets the column data types.
	 * 
	 * @return the column data types
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	private void getColumnDataTypes() throws SQLException {
		ResultSet rs = this.mcDBConnection.getMetaData().getColumns(
				null,
				XMLHelper.getAttributeAsString(this.getXMLConfig().getAttributes(), DatabaseELTWriter.SCHEMA_ATTRIB,
						null), this.mstrTableName, "%");

		boolean found = false;
		while (rs.next()) {
			found = true;
			for (int i = 0; i < this.mvColumns.size(); i++) {
				DatabaseColumnDefinition dc = (DatabaseColumnDefinition) this.mvColumns.get(i);

				if (rs.getString(4).equalsIgnoreCase(dc.getColumnName(null, this.mDBCase))) {
					dc.iSQLDataType = rs.getInt(5);
					dc.sTypeName = rs.getString("TYPE_NAME");
					dc.iSize = rs.getInt("COLUMN_SIZE");
					dc.iPrecision = rs.getInt("DECIMAL_DIGITS");
					dc.exists = true;
					for (int x = 1; x <= rs.getMetaData().getColumnCount(); x++) {
						ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, rs.getMetaData().getColumnName(x)
								+ ": " + rs.getString(x));
					}
				}
			}
		}

		if (rs != null) {
			rs.close();
		}

		if (found == false)
			throw new SQLException("Target table " + this.mstrTableName + " was not found");

	}

	/** The temp table feed. */
	static private int mTempTableFeed = 0;

	/**
	 * Gets the unique object name.
	 * 
	 * @param pPrefix
	 *            the prefix
	 * 
	 * @return the unique object name
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	private String getUniqueObjectName(String pPrefix) throws SQLException {
		boolean notFound = true;
		String res = null;
		int x = 0;

		// check for temp table existance
		while (notFound) {
			res = this.setDBCase(pPrefix + Integer.toString(x++) + "_" + DatabaseELTWriter.mTempTableFeed++);
			notFound = false;

			ResultSet rs = this.mcDBConnection.getMetaData().getTables(null, null, res, null);

			if (rs != null) {
				while (rs.next()) {
					notFound = true;
				}

				rs.close();
			}
		}

		return res;
	}

	/** The id quote. */
	private String idQuote;

	/** The id quote enabled. */
	private boolean idQuoteEnabled = false;

	/** The handle duplicate keys. */
	protected boolean mHandleDuplicateKeys = false;

	/** The statement seperator. */
	private String mStatementSeperator;

	/** The ms best join. */
	private String msBestJoin;

	/** The mb ignore invalid columns. */
	private boolean mbIgnoreInvalidColumns;

	/** The ms insert columns. */
	private String msInsertColumns;

	/** The mb replace mode. */
	private boolean mbReplaceMode;

	/** The first SK. */
	private String mFirstSK;

	/** The manage indexes. */
	private boolean mManageIndexes;

	/** The hash column. */
	private String mHashColumn;

	/** The hash column definition. */
	private DatabaseColumnDefinition mHashColumnDefinition;

	/** The mi hash fields. */
	private int[] miHashFields;

	/** The hash compare only. */
	private boolean mHashCompareOnly;

	private Properties mDatabaseProperties;

	/**
	 * Gets the ID quote.
	 * 
	 * @return the ID quote
	 */
	protected String getIDQuote() {
		if (this.idQuoteEnabled)
			return this.idQuote;

		return null;
	}

	
	private Properties getDatabaseProperties() {
		return this.mDatabaseProperties;
	}

	private void setDatabaseProperties(Map<String, Object>  parameterListValues) throws Exception {
		this.mDatabaseProperties = JDBCItemHelper.getProperties(parameterListValues);		
	}
	
	/**
	 * DOCUMENT ME!.
	 * 
	 * @param nConfig
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	public int initialize(Node nConfig) throws KETLThreadException {
		int res = super.initialize(nConfig);

		if (res != 0) {
			return res;
		}

		// Get the attributes
		NamedNodeMap nmAttrs = nConfig.getAttributes();

		// Pull the parameters from the list...
		this.strUserName = this.getParameterValue(0, DBConnection.USER_ATTRIB);
		this.strPassword = this.getParameterValue(0, DBConnection.PASSWORD_ATTRIB);
		this.strURL = this.getParameterValue(0, DBConnection.URL_ATTRIB);
		this.strDriverClass = this.getParameterValue(0, DBConnection.DRIVER_ATTRIB);
		this.strPreSQL = this.getParameterValue(0, DBConnection.PRESQL_ATTRIB);
		try {
			this.setDatabaseProperties(this.getParameterListValues(0));
		} catch (Exception e2) {
			throw new KETLThreadException(e2,this);
		}
		this.mbReinitOnError = XMLHelper.getAttributeAsBoolean(nmAttrs, "RECONNECTONERROR", true);
		this.mbReplaceMode = XMLHelper.getAttributeAsBoolean(nmAttrs, "REPLACEROWS", false);

		this.mStreamChanges = XMLHelper.getAttributeAsBoolean(nmAttrs, DatabaseELTWriter.STREAM_ATTRIB,
				this.mStreamChanges);
		this.mManageIndexes = XMLHelper.getAttributeAsBoolean(nmAttrs, "MANAGEINDEXES", false);

		this.mbIgnoreInvalidColumns = XMLHelper.getAttributeAsBoolean(nmAttrs,
				DatabaseELTWriter.IGNOREINVALIDCOLUMNS_ATTRIB, false);
		String tmpType = XMLHelper.getAttributeAsString(nmAttrs, DatabaseELTWriter.TYPE_ATTRIB, "BULK");
		this.mBatchData = XMLHelper.getAttributeAsBoolean(nmAttrs, DatabaseELTWriter.BATCH_ATTRIB, this.mBatchData);
		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Duplicates keys will be "
				+ (this.mHandleDuplicateKeys ? "handled" : "not be handled"));

		this.idQuoteEnabled = XMLHelper.getAttributeAsBoolean(nmAttrs, "IDQUOTE", false);
		String hdl = XMLHelper.getAttributeAsString(nmAttrs, DatabaseELTWriter.HANDLER_ATTRIB, null);

		this.jdbcHelper = this.instantiateHelper(hdl);

		if (tmpType.equalsIgnoreCase("ROLL"))
			this.mType = DatabaseELTWriter.ROLL;
		else if (tmpType.equalsIgnoreCase("UPSERT")) {
			this.mType = DatabaseELTWriter.UPSERT;
			this.mHandleDuplicateKeys = XMLHelper.getAttributeAsBoolean(nmAttrs,
					DatabaseELTWriter.HANDLE_DUPLICATES_ATTRIB, this.mHandleDuplicateKeys);

			this.mHashColumn = XMLHelper.getAttributeAsString(nmAttrs, DatabaseELTWriter.HASH_COLUMN_ATTRIB, null);

			if (this.mHashColumn != null)
				this.mHashCompareOnly = XMLHelper.getAttributeAsBoolean(nmAttrs,
						DatabaseELTWriter.HASH_COMPARE_ONLY_ATTRIB, false);

			if (this.mHashColumn != null) {
				this.mHashColumnDefinition = this.defineHashColumn(this.mHashColumn);
			}

		} else if (tmpType.equalsIgnoreCase("BULK"))
			this.mType = DatabaseELTWriter.BULK;

		try {
			this.mcDBConnection = ResourcePool.getConnection(this.strDriverClass, this.strURL, this.strUserName,
					this.strPassword, this.strPreSQL, true, this.getDatabaseProperties());

			this.mcDBConnection.setAutoCommit(false);
			this.mDBType = EngineConstants.cleanseDatabaseName(this.mcDBConnection.getMetaData().getDatabaseProductName());
			this.mUsedConnections.add(this.mcDBConnection);

			DatabaseMetaData md = this.mcDBConnection.getMetaData();

			this.idQuote = md.getIdentifierQuoteString();
			if (this.idQuote == null || this.idQuote.equals(" "))
				this.idQuote = "";

			if (md.storesUpperCaseIdentifiers()) {
				this.mDBCase = DatabaseELTWriter.UPPER_CASE;
			} else if (md.storesLowerCaseIdentifiers()) {
				this.mDBCase = DatabaseELTWriter.LOWER_CASE;
			} else if (md.storesMixedCaseIdentifiers()) {
				this.mDBCase = DatabaseELTWriter.MIXED_CASE;
			}
		} catch (Exception e) {
			throw new KETLThreadException(e, this);
		}

		// Pull the name of the table to be written to...
		this.mstrTableName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, DatabaseELTWriter.TABLE_ATTRIB,
				null));
		this.mstrSchemaName = this.setDBCase(XMLHelper.getAttributeAsString(nmAttrs, DatabaseELTWriter.SCHEMA_ATTRIB,
				null));

		if (this.mstrSchemaName == null)
			this.mstrSchemaName = "";
		else
			this.mstrSchemaName = this.mstrSchemaName + ".";

		// Pull the commit size...
		this.miCommitSize = XMLHelper.getAttributeAsInt(nmAttrs, DatabaseELTWriter.COMMITSIZE_ATTRIB, 20000);
		this.mLowMemoryThreashold = this.miCommitSize * 100 * this.mInPorts.length;
		this.miMaxTransactionSize = XMLHelper.getAttributeAsInt(nmAttrs, DatabaseELTWriter.MAXTRANSACTIONSIZE_ATTRIB,
				-1);
		this.mStatementSeperator = this.getStepTemplate(this.mDBType, "STATEMENTSEPERATOR", true);
		if (this.mStatementSeperator != null && this.mStatementSeperator.length() == 0)
			this.mStatementSeperator = null;

		// Convert the vector we've been building into a more common array...
		this.madcdColumns = (DatabaseColumnDefinition[]) this.mvColumns.toArray(new DatabaseColumnDefinition[0]);

		// get column datatype from the database
		try {
			this.getColumnDataTypes();

			if (this.mHashColumnDefinition != null && this.mHashColumnDefinition.exists == false) {
				String template = this.getStepTemplate(this.mDBType, "ADDHASHCOLUMN", true);

				template = EngineConstants.replaceParameterV2(template, "TABLENAME", this.mstrSchemaName
						+ this.mstrTableName);
				template = EngineConstants.replaceParameterV2(template, "COLUMNNAME", this.mstrSchemaName
						+ this.mHashColumn);

				throw new KETLThreadException("Hash column does not exist in target table, expected column "
						+ this.mHashColumnDefinition.getColumnName(this.getIDQuote(), this.mDBCase)
						+ ", execute the following SQL command to add the hash column '" + template + "'", this);
			}

			int joinKey = -1, bestJoinKey = -1;
			if (this.mPrimaryKeySpecified) {
				joinKey = DatabaseColumnDefinition.PRIMARY_KEY;
				bestJoinKey = joinKey;
			}
			// review pk choice instead of sk for updates and inserts
			if (this.mSourceKeySpecified) {
				joinKey = DatabaseColumnDefinition.SRC_UNIQUE_KEY;
				bestJoinKey = bestJoinKey == -1 ? joinKey : bestJoinKey;
			}

			StringBuffer updateColumns = new StringBuffer();
			StringBuffer insertColumns = new StringBuffer();
			StringBuffer insertSourceColumns = new StringBuffer();
			StringBuffer allColumns = new StringBuffer();
			StringBuffer join = new StringBuffer();
			StringBuffer bestjoin = new StringBuffer();
			StringBuffer updateTriggers = new StringBuffer();
			StringBuffer joinColumns = new StringBuffer();
			StringBuffer insertValues = new StringBuffer();

			String updColFormat = this.getStepTemplate(this.mDBType, "UPDATECOLUMNFORMAT", true);

			ArrayList fieldPopulationOrder = new ArrayList();
			ArrayList hashFields = new ArrayList();
			int cntJoinColumns = 0, cntInsertColumns = 0, cntUpdateTriggers = 0, cntUpdateCols = 0, cntBestJoinColumns = 0, allColumnCount = 0;

			for (int i = 0; i < this.madcdColumns.length; i++) {

				if (this.mbIgnoreInvalidColumns && this.madcdColumns[i].exists == false) {
					ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE, "Column "
							+ this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase)
							+ " not found, skipping");
					continue;
				}

				if (bestJoinKey != -1 && this.madcdColumns[i].hasProperty(bestJoinKey)) {

				} else if (joinKey != -1 && this.madcdColumns[i].hasProperty(joinKey) == false) {

					if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.UPDATE_COLUMN)) {
						if (cntUpdateCols > 0) {
							updateColumns.append(",\n\t");
						}
						cntUpdateCols++;

						String tmp = EngineConstants.replaceParameterV2(updColFormat, "TARGETCOLUMN",
								this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));

						if (this.madcdColumns[i].getAlternateUpdateValue() == null) {
							tmp = EngineConstants.replaceParameterV2(tmp, "SOURCECOLUMN", "${SOURCETABLENAME}."
									+ this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
						} else {
							tmp = EngineConstants.replaceParameterV2(tmp, "SOURCECOLUMN", this.madcdColumns[i]
									.getAlternateUpdateValue());
						}

						updateColumns.append(tmp);

					}

					if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.UPDATE_TRIGGER_COLUMN)) {

						if (this.mHashColumn == null)
							hashFields.add(i);
						else if (i != 0)
							hashFields.add(new Integer(i - 1));

						if (this.mHashColumn == null || (this.mHashCompareOnly && i == 0)
								|| (this.mHashCompareOnly == false)) {
							if (cntUpdateTriggers > 0) {
								updateTriggers.append(" OR ");
							}
							cntUpdateTriggers++;

							updateTriggers.append("((${DESTINATIONTABLENAME}.");
							updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
							updateTriggers.append(" != ${SOURCETABLENAME}.");
							updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
							updateTriggers.append(") OR (");

							if (i == 0 && this.mHashColumn != null) {
								updateTriggers.append("${DESTINATIONTABLENAME}."
										+ this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase)
										+ " is null))");
							} else {
								updateTriggers.append("${DESTINATIONTABLENAME}.");
								updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(),
										this.mDBCase));
								updateTriggers.append(" is null and ${SOURCETABLENAME}.");
								updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(),
										this.mDBCase));
								updateTriggers.append(" is not null");
								updateTriggers.append(") OR (");
								updateTriggers.append("${DESTINATIONTABLENAME}.");
								updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(),
										this.mDBCase));
								updateTriggers.append(" is not null and ${SOURCETABLENAME}.");
								updateTriggers.append(this.madcdColumns[i].getColumnName(this.getIDQuote(),
										this.mDBCase));
								updateTriggers.append(" is null))");
							}
						}
					}
				}

				if (bestJoinKey != -1 && this.madcdColumns[i].hasProperty(bestJoinKey)) {

					if (cntBestJoinColumns > 0) {
						bestjoin.append("\n\tAND ");
					} else {
						this.mFirstSK = " ${DESTINATIONTABLENAME}."
								+ this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase);
					}

					cntBestJoinColumns++;

					bestjoin.append(" ${DESTINATIONTABLENAME}.");
					bestjoin.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
					bestjoin.append(" = ${SOURCETABLENAME}.");
					bestjoin.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
				}

				if (joinKey != -1 && this.madcdColumns[i].hasProperty(joinKey)) {

					if (cntJoinColumns > 0) {
						joinColumns.append(',');
						join.append("\n\tAND ");
					}

					cntJoinColumns++;

					joinColumns.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
					join.append(" ${DESTINATIONTABLENAME}.");
					join.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
					join.append(" = ${SOURCETABLENAME}.");
					join.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
				}

				if (this.madcdColumns[i].hasProperty(DatabaseColumnDefinition.INSERT_COLUMN)) {
					if (cntInsertColumns > 0) {
						insertColumns.append(',');
						insertSourceColumns.append(',');
					}

					insertColumns.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));

					if (this.madcdColumns[i].getAlternateInsertValue() == null)
						insertSourceColumns.append("${SOURCETABLENAME}."
								+ this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
					else
						insertSourceColumns.append(this.madcdColumns[i].getAlternateInsertValue());

					cntInsertColumns++;
				}

				if (allColumnCount > 0) {
					allColumns.append(',');
					insertValues.append(',');
				}

				allColumnCount++;
				allColumns.append(this.madcdColumns[i].getColumnName(this.getIDQuote(), this.mDBCase));
				insertValues.append(this.madcdColumns[i].getParameterDefinition());

				// if hash column then first column is hash column so offset
				// columns by 1
				fieldPopulationOrder.add(this.mHashColumnDefinition == null ? new Integer(i) : i == 0 ? new Integer(-1)
						: new Integer(i - 1));
			}

			if (this.mHandleDuplicateKeys) {
				fieldPopulationOrder.add(fieldPopulationOrder.size());
				insertValues.append(",?");
			}

			this.miFieldPopulationOrder = new int[fieldPopulationOrder.size()];
			for (int i = 0; i < fieldPopulationOrder.size(); i++) {
				this.miFieldPopulationOrder[i] = ((Integer) fieldPopulationOrder.get(i)).intValue();
			}

			this.miHashFields = new int[hashFields.size()];
			for (int i = 0; i < hashFields.size(); i++) {
				this.miHashFields[i] = ((Integer) hashFields.get(i)).intValue();
			}

			this.msUpdateColumns = updateColumns.toString();
			this.setAllColumns(allColumns.toString());
			this.msJoin = join.toString();
			this.msBestJoin = bestjoin.toString();
			this.msInsertColumns = insertColumns.toString();
			this.msInsertSourceColumns = insertSourceColumns.toString();
			this.setInsertValues(insertValues.toString());
			this.msJoinColumns = joinColumns.toString();
			this.msUpdateTriggers = (cntUpdateTriggers == 0 ? "1=0" : updateTriggers.toString());
			this.getAllIndexes(XMLHelper.getAttributeAsString(nmAttrs, "JOININDEX", null));

			// if roll then table needs to be created to dump data too
			switch (this.mType) {
			case ROLL:
			case UPSERT:
				if (this.mSourceKeySpecified == false) {
					ResourcePool
							.LogMessage(this, ResourcePool.ERROR_MESSAGE,
									"Insert type requires source keys to be specified, either specify them or switch to BULK insert");
					return -1;
				}

				this.msTempTableName = this.getUniqueObjectName("t");
				this.maOtherColumns = this.getAllOtherTableColumns();
				break;
			case BULK:
				this.msTempTableName = this.mstrTableName;
				break;
			}

			this.msPreLoadSQL = this.buildPreLoadSQL();
			this.msPostLoadSQL = this.buildPostLoadSQL();
			this.msInBatchSQLStatement = this.buildInBatchSQL(this.mstrSchemaName + this.msTempTableName);

			if (this.debug())
				ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Insert statement: "
						+ this.msInBatchSQLStatement);
			this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement, this.jdbcHelper);

			this.maxCharLength = this.mcDBConnection.getMetaData().getMaxCharLiteralLength();
			this.supportsSetSavepoint = this.mcDBConnection.getMetaData().supportsSavepoints();

			if (this.supportsSetSavepoint) {
				Savepoint sPoint = null;
				try {
					sPoint = this.mcDBConnection.setSavepoint();
				} catch (SQLException e) {
					this.supportsSetSavepoint = false;
				}

				if (sPoint != null) {
					try {
						this.mcDBConnection.releaseSavepoint(sPoint);
						this.supportsReleaseSavepoint = true;
					} catch (SQLException e) {
						this.supportsReleaseSavepoint = false;
					}
				}

			}
			this.msPostBatchSQL = this.buildPostBatchSQL();

			this.mIncrementalCommit = XMLHelper.getAttributeAsBoolean(nmAttrs, "INCREMENTALCOMMIT", true);

			if (this.mIncrementalCommit == false && this.supportsSetSavepoint == false) {
				throw new KETLThreadException(
						"Incremental commit cannot be disabled for database's that do not support savepoints", this);
			}
			this.miRetryBatch = XMLHelper.getAttributeAsInt(nmAttrs, "RETRYBATCH", 1);

			this.executePreStatements();

		} catch (Exception e) {
			try {
				StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
						this.mStatementSeperator, StatementManager.END, this, true);
			} catch (Exception e1) {

			}
			throw new KETLThreadException(e, this);
		}

		return 0;
	}

	/**
	 * Gets the all indexes.
	 * 
	 * @param string
	 *            the string
	 * 
	 * @return the all indexes
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	private void getAllIndexes(String string) throws SQLException, KETLThreadException {

		if (this.mManageIndexes == false)
			return;

		ResultSet indexRs = this.mcDBConnection.getMetaData().getIndexInfo(null, this.mstrSchemaName.length()==0?null:this.mstrSchemaName,
				this.mstrTableName, false, true);
		ArrayList indexList = new ArrayList();
		while (indexRs.next()) {
			String idxName = indexRs.getString(6);

			if (idxName != null && idxName.equalsIgnoreCase(string) == false)
				indexList.add(idxName);
		}

		indexRs.close();

		if (this.partitionID == 0) {
			String idxEnable = this.getStepTemplate(this.mDBType, "ENABLEINDEX", true);

			if (idxEnable != null && idxEnable.length() > 0) {
				for (Object o : indexList) {
					this.mIndexEnableList.add(EngineConstants.replaceParameterV2(idxEnable, "INDEXNAME", (String) o));
				}
			}
		}

		if (this.partitionID == 0) {
			String idxDisable = this.getStepTemplate(this.mDBType, "DISABLEINDEX", true);

			if (idxDisable != null && idxDisable.length() > 0) {
				for (Object o : indexList) {
					this.mIndexDisableList.add(EngineConstants.replaceParameterV2(idxDisable, "INDEXNAME", (String) o));
				}
			}
		}
	}

	/**
	 * Instantiate helper.
	 * 
	 * @param hdl
	 *            the hdl
	 * 
	 * @return the JDBC item helper
	 * 
	 * @throws KETLThreadException
	 *             the KETL thread exception
	 */
	abstract protected JDBCItemHelper instantiateHelper(String hdl) throws KETLThreadException;

	/**
	 * Sets the DB case.
	 * 
	 * @param pStr
	 *            the str
	 * 
	 * @return the string
	 */
	private String setDBCase(String pStr) {

		if (pStr == null)
			return null;

		switch (this.mDBCase) {
		case LOWER_CASE:
			return pStr.toLowerCase();

		case MIXED_CASE:
			return pStr;

		case UPPER_CASE:
			return pStr.toUpperCase();
		}

		return pStr;
	}

	/** The dedupe counter. */
	protected int dedupeCounter = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.DefaultWriterCore#putNextRecord(java.lang.Object[],
	 *      java.lang.Class[], int)
	 */
	public int putNextRecord(Object[] pInputRecords, Class[] pExpectedDataTypes, int pRecordWidth)
			throws KETLWriteException {

		int res = 1;
		try {
			int cols = this.miFieldPopulationOrder.length;
			for (int i = 1; i <= cols; i++) {
				if (this.mHandleDuplicateKeys && i == cols) {
					this.stmt.setParameterFromClass(i, Integer.class, this.dedupeCounter++, this.maxCharLength, null);
				} else {
					Class datumClass;

					int fieldID = this.miFieldPopulationOrder[i - 1];

					if (fieldID == -1 && this.mHashColumn != null) {
						this.stmt.setParameterFromClass(i, Integer.class, this.getHashCode(pInputRecords),
								this.maxCharLength, null);
					} else {
						ETLInPort port = this.mInPorts[fieldID];
						int idx = -1;
						if (port.isConstant())
							datumClass = port.getPortClass();
						else {
							idx = port.getSourcePortIndex();
							datumClass = pExpectedDataTypes[idx];
						}

						try {
							this.stmt.setParameterFromClass(i, datumClass, port.isConstant() ? port.getConstantValue()
									: pInputRecords[idx], this.maxCharLength, port.getXMLConfig());
						} catch (ClassCastException e1) {
							throw new KETLWriteException("Error with port "
									+ port.mstrName
									+ " expected datatype "
									+ datumClass.getCanonicalName()
									+ " incoming datatype was "
									+ (port.isConstant() ? port.getPortClass() : pInputRecords[idx].getClass()
											.getCanonicalName()));
						}
					}
				}

			}

			if (this.mBatchData) {
				this.stmt.addBatch();
				this.logBatch(pInputRecords);

				this.mBatchCounter++;
			} else {
				res = this.stmt.executeUpdate();
			}
		} catch (SQLException e) {
			throw new KETLWriteException(e);
		}

		return res;
	}

	/** The batch log. */
	ArrayList mBatchLog = new ArrayList();

	/** The record num batch start. */
	int recordNumBatchStart;

	/**
	 * Log batch.
	 * 
	 * @param inputRecords
	 *            the input records
	 */
	private void logBatch(Object[] inputRecords) {
		this.mBatchLog.add(inputRecords);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.DBConnection#getConnection()
	 */
	public Connection getConnection() {
		return this.mcDBConnection;
	}

	/** The mi retry batch. */
	int miRetryBatch = 0;

	/** The incremental commit. */
	boolean mIncrementalCommit = true;

	/**
	 * Prepare statement wrapper.
	 * 
	 * @param Connection
	 *            the connection
	 * @param sql
	 *            the sql
	 * @param jdbcHelper
	 *            the jdbc helper
	 * 
	 * @return the statement wrapper
	 * 
	 * @throws SQLException
	 *             the SQL exception
	 */
	abstract StatementWrapper prepareStatementWrapper(Connection Connection, String sql, JDBCItemHelper jdbcHelper)
			throws SQLException;

	/**
	 * Retry batch.
	 * 
	 * @return the int
	 * 
	 * @throws KETLWriteException
	 *             the KETL write exception
	 */
	private int retryBatch() throws KETLWriteException {
		ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
				"Retrying records in batch, to identify invalid records");
		int result = 0;
		for (int r = 0; r < this.miRetryBatch; r++) {
			int errorCount = 0, submitted = 0;

			// reset statement as some drivers fail after a failure has occured
			try {
				this.stmt.close();
				this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement,
						this.jdbcHelper);
			} catch (SQLException e) {
				throw new KETLWriteException(e);
			}
			for (int x = 0; x < this.mBatchLog.size(); x++) {
				Object[] record = (Object[]) this.mBatchLog.get(x);

				if (this.mFailedBatchElements.contains(x)) {
					try {
						if (this.mbReinitOnError) {
							this.stmt.close();
							this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement,
									this.jdbcHelper);
						}

						int cols = this.miFieldPopulationOrder.length;
						for (int i = 1; i <= cols; i++) {
							if (this.mHandleDuplicateKeys && i == cols) {
								this.stmt.setParameterFromClass(i, Integer.class, this.dedupeCounter++,
										this.maxCharLength, null);
							} else {
								Class datumClass;
								int fieldID = this.miFieldPopulationOrder[i - 1];

								if (fieldID == -1 && this.mHashColumn != null) {
									this.stmt.setParameterFromClass(i, Integer.class, this.getHashCode(record),
											this.maxCharLength, null);

								} else {
									ETLInPort port = this.mInPorts[fieldID];
									int idx = -1;
									if (port.isConstant())
										datumClass = port.getPortClass();
									else {
										idx = port.getSourcePortIndex();
										datumClass = this.getExpectedDataTypes()[idx];
									}

									this.stmt.setParameterFromClass(i, datumClass, port.isConstant() ? port
											.getConstantValue() : record[idx], this.maxCharLength, port.getXMLConfig());
								}
							}
						}

						submitted++;
						this.stmt.executeUpdate();
						result++;
						this.mFailedBatchElements.remove(x);
						if (this.mIncrementalCommit)
							this.mcDBConnection.commit();

					} catch (SQLException e) {
						errorCount++;
						if (r == this.miRetryBatch - 1)
							this.incrementErrorCount(new KETLWriteException("Record " + (this.miInsertCount + x + 1)
									+ " failed to submit, " + e.toString(), e), record, this.miInsertCount + x + 1);

						try {
							this.stmt.close();
							this.stmt = this.prepareStatementWrapper(this.mcDBConnection, this.msInBatchSQLStatement,
									this.jdbcHelper);
						} catch (SQLException e1) {
							throw new KETLWriteException(e1);
						}

					}
				}
			}

			ResourcePool.LogMessage(this, ResourcePool.WARNING_MESSAGE, "Batch retry attempt " + (r + 1) + " of "
					+ this.miRetryBatch + ", Records resubmitted: " + submitted + ", errors: " + errorCount);
		}

		return result;

	}

	/**
	 * Gets the hash code.
	 * 
	 * @param record
	 *            the record
	 * 
	 * @return the hash code
	 */
	private int getHashCode(Object[] record) {

		if (record == null)
			return 0;

		int result = 1;

		for (int element : this.miHashFields) {
			ETLInPort port = this.mInPorts[element];
			int hash;
			if (port.isConstant())
				hash = port.getConstantValue().hashCode();
			else {
				Object obj = record[port.getSourcePortIndex()];
				hash = obj == null ? 0 : obj.hashCode();
			}

			result = 31 * result + hash;
		}
		return result;
	}

	/** The low memory threashold. */
	long mLowMemoryThreashold = -1;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.WriterBatchManager#finishBatch(int)
	 */
	public int finishBatch(int len) throws KETLWriteException {
		int result = 0;
		try {
			if (this.mBatchData
					&& (this.mBatchCounter >= this.miCommitSize
							|| (this.mBatchCounter > 0 && this.isMemoryLow(this.mLowMemoryThreashold)) || (len == BatchManager.LASTBATCH && this.mBatchCounter > 0))) {
				this.dedupeCounter = 0;
				boolean errorsOccured = false;
				Savepoint savepoint = null;
				try {

					if (this.supportsSetSavepoint) {
						savepoint = this.mcDBConnection.setSavepoint();
					}

					Exception e1 = null;
					int[] res = null;
					try {
						if (this.debug())
							ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Submitting batch, size: "
									+ this.mBatchCounter);
						res = this.stmt.executeBatch();

						if (this.debug())
							ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Batched submitted succesfully");

						if (this.supportsReleaseSavepoint && savepoint != null) {
							this.mcDBConnection.releaseSavepoint(savepoint);
						}

					} catch (BatchUpdateException e) {
						if (savepoint != null)
							this.mcDBConnection.rollback(savepoint);
						else
							res = e.getUpdateCounts();
						e1 = e;
						errorsOccured = true;
					}

					if (errorsOccured && res == null) {
						for (int i = 0; i < this.mBatchLog.size(); i++) {
							if (this.miRetryBatch == 0)
								this.incrementErrorCount(e1 == null ? new KETLWriteException("Failed to submit record "
										+ (i + 1 + this.miInsertCount)) : new KETLWriteException(e1),
										(Object[]) this.mBatchLog.get(i), i + 1 + this.miInsertCount);
							else
								this.mFailedBatchElements.add(i);
						}
					} else {
						int rLen = res.length;
						for (int i = 0; i < rLen; i++) {
							if (res[i] == Statement.EXECUTE_FAILED) {
								this.mFailedBatchElements.add(i);
								if (this.miRetryBatch == 0)
									this.incrementErrorCount(e1 == null ? new KETLWriteException(
											"Failed to submit record " + (i + 1 + this.miInsertCount))
											: new KETLWriteException(e1), (Object[]) this.mBatchLog.get(rLen), i + 1
											+ this.miInsertCount);
							} else {
								result += res[i] >= 0 ? res[i] : 1;
							}
						}
					}
				} catch (SQLException e) {
					throw new KETLWriteException(e);
				}

				if (errorsOccured && this.miRetryBatch > 0) {
					result = this.retryBatch();
				}

				this.clearBatchLogBatch();

				this.miInsertCount += this.mBatchCounter;
				this.mBatchCounter = 0;

				if (this.mIncrementalCommit)
					this.mcDBConnection.commit();
				this.executePostBatchStatements();
				this.firePreBatch = true;

			} else if (this.mBatchData == false) {
				if (this.mIncrementalCommit)
					this.mcDBConnection.commit();
			}

		} catch (SQLException e) {
			try {
				// StatementManager.executeStatements(this.getFailureCleanupLoadSQL(),
				// this.mcDBConnection,
				// mStatementSeperator, StatementManager.END, this, true);
			} catch (Exception e1) {

			}

			KETLWriteException e1 = new KETLWriteException(e);
			this.incrementErrorCount(e1, null, 1);

			throw e1;
		}

		return result;
	}

	/** The failed batch elements. */
	private Set mFailedBatchElements = new HashSet();

	/**
	 * Clear batch log batch.
	 */
	protected void clearBatchLogBatch() {
		this.mBatchLog.clear();
		this.mFailedBatchElements.clear();
	}

	/** The supports release savepoint. */
	boolean supportsSetSavepoint = false, supportsReleaseSavepoint = false;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.WriterBatchManager#initializeBatch(java.lang.Object[][],
	 *      int)
	 */
	public Object[][] initializeBatch(Object[][] data, int len) throws KETLWriteException {
		try {
			if (this.firePreBatch && this.mBatchData) {
				this.executePreBatchStatements();
				this.recordNumBatchStart = this.getRecordsProcessed();
				this.firePreBatch = false;
			}

		} catch (SQLException e) {
			throw new KETLWriteException(e);
		}
		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePostStatements()
	 */
	public void executePostStatements() throws SQLException {
		this.setWaiting("post statements to run");
		StatementManager.executeStatements(this.msPostLoadSQL, this.mcDBConnection, this.mStatementSeperator,
				StatementManager.END, this, false);

		if (this.mType == DatabaseELTWriter.UPSERT && this.miAnalyzePos != -1) {
			// remove analyzes they should only happen once
			this.msPostLoadSQL[this.miAnalyzePos] = null;
			this.miAnalyzePos = -1;
		}

		if (this.isLastThreadToEnterCompletePhase()) {
			// wait for all other threads to complete
			this.setWaiting("all other threads in group to complete");
			while (this.getThreadManager().countOfStepThreadsAlive(this) > 1) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new KETLError(e);
				}
			}
			this.setWaiting("final statements to run");

		}

		if (this.isLastThreadToEnterCompletePhase()) {
			this.setWaiting("indexes to rebuild");
			StatementManager.executeStatements(this.mIndexEnableList.toArray(), this.mcDBConnection,
					this.mStatementSeperator, StatementManager.END, this, false);
		}
		StatementManager.executeStatements(this, this, "POSTSQL", StatementManager.END);
		this.setWaiting(null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreStatements()
	 */
	public void executePreStatements() throws SQLException {
		this.setWaiting("pre statements to run");
		StatementManager.executeStatements(this, this, "PRESQL", StatementManager.START);
		StatementManager.executeStatements(this.msPreLoadSQL, this.mcDBConnection, this.mStatementSeperator,
				StatementManager.START, this, false);

		if (this.isFirstThreadToEnterInitializePhase()) {
			this.setWaiting("indexes to be disabled");
			StatementManager.executeStatements(this.mIndexEnableList.toArray(), this.mcDBConnection,
					this.mStatementSeperator, StatementManager.END, this, false);
		}
		this.setWaiting(null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePostBatchStatements()
	 */
	public void executePostBatchStatements() throws SQLException {
		this.setWaiting("post batch statements to run");
		StatementManager.executeStatements(this.msPostBatchSQL, this.mcDBConnection, this.mStatementSeperator,
				StatementManager.END, this, false);
		StatementManager.executeStatements(this, this, "POSTBATCHSQL");
		this.setWaiting(null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.dbutils.PrePostSQL#executePreBatchStatements()
	 */
	public void executePreBatchStatements() throws SQLException {
		this.setWaiting("pre batch statements to run");
		StatementManager.executeStatements(this, this, "PREBATCHSQL");
		this.setWaiting(null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#getNewInPort(com.kni.etl.ketl.ETLStep)
	 */
	@Override
	protected ETLInPort getNewInPort(ETLStep srcStep) {
		return new JDBCETLInPort(this, srcStep);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.kni.etl.ketl.smp.ETLWorker#close(boolean)
	 */
	@Override
	protected void close(boolean success) {
		try {

			if (this.mcDBConnection != null && this.mIncrementalCommit == false && success == false
					&& this.getRecordsProcessed() > 0) {
				this.mcDBConnection.rollback();
			}

		} catch (SQLException e) {
			ResourcePool.LogException(e, this);
		}
		try {

			if (this.stmt != null)
				this.stmt.close();
		} catch (SQLException e) {
			ResourcePool.LogException(e, this);
		}
		if (this.mcDBConnection != null) {
			if (success == false && this.debug() == false) {
				try {
					StatementManager.executeStatements(this.getFailureCleanupLoadSQL(), this.mcDBConnection,
							this.mStatementSeperator, StatementManager.END, this, true);
				} catch (Exception e1) {
				}
			}
			ResourcePool.releaseConnection(this.mcDBConnection);
		}
	}

	/**
	 * Sets the all columns.
	 * 
	 * @param msAllColumns
	 *            the new all columns
	 */
	void setAllColumns(String msAllColumns) {
		this.msAllColumns = msAllColumns;
	}

	/**
	 * Gets the all columns.
	 * 
	 * @return the all columns
	 */
	String getAllColumns() {
		return this.msAllColumns;
	}

	/**
	 * Sets the insert values.
	 * 
	 * @param msInsertValues
	 *            the new insert values
	 */
	void setInsertValues(String msInsertValues) {
		this.msInsertValues = msInsertValues;
	}

	/**
	 * Gets the insert values.
	 * 
	 * @return the insert values
	 */
	String getInsertValues() {
		return this.msInsertValues;
	}

}
