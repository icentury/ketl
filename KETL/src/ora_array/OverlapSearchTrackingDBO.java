/**
 * $Id: OverlapSearchTrackingDBO.java,v 1.1 2006/12/13 07:06:42 nwakefield Exp $ */
package ora_array;


import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Timestamp;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.OracleSQLOutput;

public class OverlapSearchTrackingDBO implements SQLData
{
  public static final String SQL_TYPE_NAME = "OVERLAP_SEARCH_TRK_T";
  public static final String SQL_ARRAY_TYPE_NAME = "OVERLAP_SEARCH_TRK_ARRAY_T";

  private int _sessionID;

  private int _memberID;

  private int _pageSeq;

  private int _queryProposalAccepts;

  private QueryEntryDBO[] _queryEntries;

  private String _queryOriginType;

  private String _queryOriginName;

  private int _numberOfResults;

  private long _computationTime;

  private long _trackingTime;

  public OverlapSearchTrackingDBO(
          int sessionID,
          int memberID,
          int pageSeq,
          int queryProposalAccepts,
          QueryEntryDBO[] queryEntries,
          String queryOriginType,
          String queryOriginName,
          int numberOfResults,
          long computationTime,
          long trackingTime)
  {
    this._sessionID = sessionID;
    this._memberID = memberID;
    this._pageSeq = pageSeq;
    this._queryProposalAccepts = queryProposalAccepts;
    this._queryEntries = queryEntries;
    this._queryOriginType = queryOriginType;
    this._queryOriginName = queryOriginName;
    this._numberOfResults = numberOfResults;
    this._computationTime = computationTime;
    this._trackingTime = trackingTime;
  }


  /**
   * Access function */
  public int getSessionID()
  {
    return this._sessionID;
  }

  /**
   * Access function */
  public int getMemberID()
  {
    return this._memberID;
  }

  /**
   * Access function */
  public int getPageSeq()
  {
    return this._pageSeq;
  }

  /**
   * Access function */
  public int getQueryProposalAccepts()
  {
    return this._queryProposalAccepts;
  }

  /**
   * Access function */
  public QueryEntryDBO[] getQueryEntries()
  {
    return this._queryEntries;
  }

  /**
   * Access function */
  public String getQueryOriginType()
  {
    return this._queryOriginType;
  }

  /**
   * Access function */
  public String getQueryOriginName()
  {
    return this._queryOriginName;
  }

  /**
   * Access function */
  public int getNumberOfResults()
  {
    return this._numberOfResults;
  }

  /**
   * Access function */
  public long getComputationTime()
  {
    return this._computationTime;
  }

  /**
   * Access function */
  public long getTrackingTime()
  {
    return this._trackingTime;
  }

  public String getSQLTypeName() throws SQLException
  {
    return OverlapSearchTrackingDBO.SQL_TYPE_NAME;
  }

  public void readSQL(SQLInput stream, String type) throws SQLException
  {
    // not used
    throw new RuntimeException("not used");
  }

  public void writeSQL(SQLOutput stream) throws SQLException
  {
    stream.writeInt(this._sessionID);
    stream.writeInt(this._memberID);
    stream.writeInt(this._pageSeq);
    this.writeArray(stream, QueryEntryDBO.SQL_ARRAY_TYPE_NAME, this._queryEntries);
    stream.writeInt(this._queryProposalAccepts);
    stream.writeString(this._queryOriginType);
    stream.writeInt(this._numberOfResults);
    stream.writeLong(this._computationTime);
    stream.writeTimestamp(new Timestamp(this._trackingTime));
    stream.writeString(this._queryOriginName);
  }

  public <T> void writeArray(SQLOutput stream,
                             String arrayTypeName,
                             T[] array) throws SQLException
  {
    OracleSQLOutput out = (OracleSQLOutput) stream;
    Connection db = out.getSTRUCT().getConnection();

    ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor(arrayTypeName, db);
    stream.writeArray(new ARRAY(descriptor, db, array));
  }

}