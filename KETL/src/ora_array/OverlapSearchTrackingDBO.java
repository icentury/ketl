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
    _sessionID = sessionID;
    _memberID = memberID;
    _pageSeq = pageSeq;
    _queryProposalAccepts = queryProposalAccepts;
    _queryEntries = queryEntries;
    _queryOriginType = queryOriginType;
    _queryOriginName = queryOriginName;
    _numberOfResults = numberOfResults;
    _computationTime = computationTime;
    _trackingTime = trackingTime;
  }


  /**
   * Access function */
  public int getSessionID()
  {
    return _sessionID;
  }

  /**
   * Access function */
  public int getMemberID()
  {
    return _memberID;
  }

  /**
   * Access function */
  public int getPageSeq()
  {
    return _pageSeq;
  }

  /**
   * Access function */
  public int getQueryProposalAccepts()
  {
    return _queryProposalAccepts;
  }

  /**
   * Access function */
  public QueryEntryDBO[] getQueryEntries()
  {
    return _queryEntries;
  }

  /**
   * Access function */
  public String getQueryOriginType()
  {
    return _queryOriginType;
  }

  /**
   * Access function */
  public String getQueryOriginName()
  {
    return _queryOriginName;
  }

  /**
   * Access function */
  public int getNumberOfResults()
  {
    return _numberOfResults;
  }

  /**
   * Access function */
  public long getComputationTime()
  {
    return _computationTime;
  }

  /**
   * Access function */
  public long getTrackingTime()
  {
    return _trackingTime;
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
    stream.writeInt(_sessionID);
    stream.writeInt(_memberID);
    stream.writeInt(_pageSeq);
    writeArray(stream, QueryEntryDBO.SQL_ARRAY_TYPE_NAME, _queryEntries);
    stream.writeInt(_queryProposalAccepts);
    stream.writeString(_queryOriginType);
    stream.writeInt(_numberOfResults);
    stream.writeLong(_computationTime);
    stream.writeTimestamp(new Timestamp(_trackingTime));
    stream.writeString(_queryOriginName);
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