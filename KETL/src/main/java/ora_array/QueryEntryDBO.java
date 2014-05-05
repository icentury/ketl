/**
 * $Id: QueryEntryDBO.java,v 1.1 2006/12/13 07:06:41 nwakefield Exp $ */
package ora_array;

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

public class QueryEntryDBO implements SQLData
{
  public static final String SQL_TYPE_NAME = "QUERY_ENTRY_T";
  public static final String SQL_ARRAY_TYPE_NAME = "QUERY_ENTRY_ARRAY_T";

  /**
   * name of company searched for
   */
  private String _company;

  /**
   * start year for search
   */
  private Integer _startYear;

  /**
   * end year for search
   */
  private Integer _endYear;

  /**
   * @param company   name of company searched for
   * @param startYear start year for search
   * @param endYear   end year for search
   */
  public QueryEntryDBO(String company,
                       Integer startYear,
                       Integer endYear)
  {
    this._company = company;
    this._startYear = startYear;
    this._endYear = endYear;
  }

  /**
   * @return name of company searched for
   */
  public String getCompany()
  {
    return this._company;
  }

  /**
   * @return start year for search
   */
  public Integer getStartYear()
  {
    return this._startYear;
  }

  /**
   * @return end year for search
   */
  public Integer getEndYear()
  {
    return this._endYear;
  }

  public String getSQLTypeName() throws SQLException
  {
    return QueryEntryDBO.SQL_TYPE_NAME;
  }

  public void readSQL(SQLInput stream, String type) throws SQLException
  {
    // not used
    throw new RuntimeException("not used");
  }

  public void writeSQL(SQLOutput stream) throws SQLException
  {
    stream.writeString(this._company);
    QueryEntryDBO.writeIntegerObject(stream, this._startYear);
    QueryEntryDBO.writeIntegerObject(stream, this._endYear);
  }

  /**
   * Sets the Integer value on the stream
   */
  public static void writeIntegerObject(SQLOutput stream, Integer value)
    throws SQLException
  {
    if(value == null)
    {
      stream.writeObject(null);
    }
    else
    {
      stream.writeInt(value.intValue());
    }
  }

}
