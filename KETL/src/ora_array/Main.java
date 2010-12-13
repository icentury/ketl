/**
 * $Id: Main.java,v 1.1 2006/12/13 07:06:41 nwakefield Exp $ */
package ora_array;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

/**
 * @author Yan Pujante
 * @version $Revision$
 */
public class Main
{
  public static void main(String[] args) throws Exception
  {
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Connection db = DriverManager.getConnection("jdbc:oracle:thin:leo2/leo@localhost:1521:DB");

    OverlapSearchTrackingDBO[] dbos = {
      new OverlapSearchTrackingDBO(100,
                                   1,
                                   1,
                                   0,
                                   new QueryEntryDBO[] {new QueryEntryDBO("abc", 100, 200)},
                                   "O",
                                   "O",
                                   0,
                                   0,
                                   System.currentTimeMillis())
    };

    try
    {
      String sql = "{?= call insert_overlap_search_tracking(?)}";

      CallableStatement stmt = db.prepareCall(sql);

      try
      {
        stmt.registerOutParameter(1, Types.INTEGER);
        ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor(OverlapSearchTrackingDBO.SQL_ARRAY_TYPE_NAME, db);
        stmt.setObject(2, new ARRAY(descriptor, db, dbos));
        stmt.execute();
      }
      finally
      {
        stmt.close();
      }
    }
    finally
    {
      db.close();
    }
  }


  /**
   * Constructor
   */
  public Main()
  {
  }
}
