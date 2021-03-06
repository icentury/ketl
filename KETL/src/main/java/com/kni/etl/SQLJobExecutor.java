/*
 * Copyright (C) May 11, 2007 Kinetic Networks, Inc. All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307, USA.
 * 
 * Kinetic Networks Inc 33 New Montgomery, Suite 1200 San Francisco CA 94105
 * http://www.kineticnetworks.com
 */
package com.kni.etl;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.kni.etl.dbutils.ResourcePool;
import com.kni.etl.util.XMLHelper;
import com.kni.util.ExternalJarLoader;

// TODO: Auto-generated Javadoc
/**
 * The Class SQLJobExecutor.
 */
public class SQLJobExecutor extends ETLJobExecutor {

  /** The monitor. */
  private SQLJobMonitor monitor;

  /**
   * The Class SQLJobMonitor.
   */
  private class SQLJobMonitor extends Thread {

    /** The alive. */
    boolean alive = true;

    /** The stmt. */
    public Statement stmt = null;;

    public SQLJobMonitor(ETLJob ejJob) {
      this.setName("SQL Job Monitor - " + ejJob.getJobID());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        int cnt = 0;
        while (this.alive) {

          ETLJob job = sjJob;
          if (this.stmt != null && job != null && job.isCancelled()) {
            try {
              this.stmt.cancel();
              job.cancelSuccessfull(true);
            } catch (SQLException e) {
              ResourcePool.LogException(e, this);
            }

          }

          if (cnt % 8 == 0 && cnt > 0) {
            long runTime = (System.currentTimeMillis() - startTime) / 1000;

            job.getStatus().setExtendedMessage(
                ("Running statement(" + runTime + "s):" + (curSQL == null ? "n/a" : curSQL)));

            if (runTime > job.getTimeout()) {
              job.getStatus().setExtendedMessage(
                  "Job being failed timeout exceeded of " + job.getTimeout() + "s");
              if (this.stmt != null) {
                try {
                  stmt.cancel();
                } catch (SQLException e) {
                  ResourcePool.LogException(e, this);
                }
              } else {

              }
            }

          }
          Thread.sleep(500);
          cnt++;

        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * SQLJobExecutor constructor comment.
   */
  public SQLJobExecutor() {
    super();
  }

  /** The db connection. */
  Connection dbConnection = null;

  /**
   * Wrap execution.
   * 
   * @param stmt the stmt
   * @param curSQL the cur SQL
   * @param debug the debug
   * @return the long
   * @throws SQLException the SQL exception
   */
  private long wrapExecution(Statement stmt, String curSQL, boolean debug) throws SQLException {
    long start = (System.currentTimeMillis() - 1);
    if (debug)
      ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Executing statement: " + curSQL);

    this.monitor.stmt = stmt;
    try {
      stmt.execute(curSQL);
    } finally {
      this.monitor.stmt = null;
    }
    if (debug)
      ResourcePool.LogMessage(this, ResourcePool.DEBUG_MESSAGE, "Execution time(Seconds): "
          + (((double) (System.currentTimeMillis() - start)) / (double) 1000));

    return System.currentTimeMillis() - start;
  }

  private SQLJob sjJob;
  private String curSQL;

  /**
   * Insert the method's description here. Creation date: (5/4/2002 5:37:52 PM)
   * 
   * @param ejJob the ej job
   * @return boolean
   */
  @Override
  protected boolean executeJob(ETLJob ejJob) {
    this.monitor = new SQLJobMonitor(ejJob);

    try {
      curSQL = null;

      this.monitor.start();

      ETLStatus jsJobStatus;
      // Only accept SQL jobs...
      if ((ejJob instanceof SQLJob) == false) {
        return false;
      }

      sjJob = (SQLJob) ejJob;
      jsJobStatus = sjJob.getStatus();

      // Get a connection to our database...
      try {
        this.dbConnection =
            ResourcePool.getConnection(sjJob.getDatabaseDriverClass(), sjJob.getDatabaseURL(),
                sjJob.getDatabaseUser(), sjJob.getDatabasePassword(), sjJob.getPreSQL(), true,
                sjJob.getDatabaseProperties());
      } catch (Exception e) {
        jsJobStatus.setErrorCode(1); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES
        jsJobStatus.setErrorMessage("Error connecting to database: " + e.getMessage());
      }

      // If we had an error connecting, return failure...
      if (this.dbConnection == null) {
        return false;
      }

      int statement = -1;

      // Run the SQL job
      try {
        long start = (System.currentTimeMillis() - 1);

        Statement stmt = this.dbConnection.createStatement();

        boolean defaultDebug =
            sjJob.mSQLNode == null ? false : XMLHelper.getAttributeAsBoolean(
                sjJob.mSQLNode.getAttributes(), "DEBUG", false);

        if (sjJob.mSQLNode == null) {
          // a little safety measure, defaults to 10000 or specified value
          this.getStatus().setExtendedMessage("Running statement 1 of 1");

          stmt.setMaxRows(sjJob.getMaxRows());
          curSQL = sjJob.getSQL();
          long executionTime = this.wrapExecution(stmt, curSQL, false);
          curSQL = null;
          sjJob.setResultSet(stmt.getResultSet());
          sjJob.setUpdateCount(stmt.getUpdateCount());
          sjJob.getStatus().setStats(statement, stmt.getUpdateCount(), executionTime);
        } else {
          NodeList nl = sjJob.mSQLNode.getChildNodes();

          if ((nl == null) || (nl.getLength() == 0)) {
            jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES
            jsJobStatus.setErrorMessage("Error running SQL: no sql statements specified in XML");
          } else {
            ArrayList nodes = new ArrayList();
            for (statement = 0; statement < nl.getLength(); statement++) {
              Node n = nl.item(statement);

              if (n.getNodeType() == Node.ELEMENT_NODE) {
                nodes.add(n);
              }
            }

            statement = 0;
            for (Object o : nodes) {
              Node n = (Node) o;
              statement++;
              this.getStatus().setExtendedMessage(
                  "Running statement " + (nodes.indexOf(o) + 1) + " of " + nodes.size());

              boolean autoCommit =
                  XMLHelper.getAttributeAsBoolean(n.getAttributes(), SQLJob.AUTOCOMMIT, true);

              if (autoCommit != this.dbConnection.getAutoCommit()) {
                this.dbConnection.setAutoCommit(autoCommit);
              }

              sjJob.setMaxRows(XMLHelper.getAttributeAsInt(n.getAttributes(), SQLJob.MAXROWS,
                  sjJob.getMaxRows()));

              stmt.setMaxRows(sjJob.getMaxRows());

              String paramName =
                  XMLHelper.getAttributeAsString(n.getAttributes(), ETLJob.WRITEBACK_PARAMETER,
                      null);
              boolean critical =
                  XMLHelper.getAttributeAsBoolean(n.getAttributes(), "CRITICAL", true);
              boolean debug =
                  XMLHelper.getAttributeAsBoolean(n.getAttributes(), "DEBUG", defaultDebug);

              if ((curSQL = getExternalSourceCode(n)) == null)
                curSQL = sjJob.resolveParameters(XMLHelper.getTextContent(n));
              else
                curSQL = sjJob.resolveParameters(curSQL);

              try {
                long executionTime = this.wrapExecution(stmt, curSQL, debug);

                curSQL = null;

                String jobTriggers =
                    XMLHelper.getAttributeAsString(n.getAttributes(), "JOBTRIGGERS", null);
                String value = null;
                if (paramName != null || jobTriggers != null) {
                  ResultSet rs = stmt.getResultSet();
                  try {
                    int cols = rs.getMetaData().getColumnCount();

                    if (cols != 1) {
                      throw new Exception(
                          "For parameter writeback or triggers only one column is expected, the following SQL returns more than one column -"
                              + n.getFirstChild().getNodeValue());
                    }

                    while (rs.next()) {
                      value = rs.getString(1);
                    }
                  } finally {
                    rs.close();
                  }
                }

                if (jobTriggers != null) {
                  try {
                    this.fireJobTriggers(sjJob.iLoadID, jobTriggers, value);
                  } catch (Exception e) {
                    ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.ERROR_MESSAGE,
                        "Error firing triggers, check format <EXITCODE>=<VALUE>=(exec|setStatus)(..);... : "
                            + e.getMessage());
                  }
                }
                if (paramName != null) {

                  setWritebackParameter(XMLHelper.getAttributeAsString(n.getAttributes(),
                      EngineConstants.PARAMETER_LIST, null), paramName, value);
                } else if (nl.getLength() == 1) {
                  sjJob.setResultSet(stmt.getResultSet());
                }

                sjJob.setUpdateCount(sjJob.getUpdateCount() + stmt.getUpdateCount());
                sjJob.getStatus().setStats(statement, stmt.getUpdateCount(), executionTime);
              } catch (SQLException e) {
                if (critical)
                  throw e;
                ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.INFO_MESSAGE,
                    "IGNORE:" + e.getMessage());
                try {
                  stmt.getConnection().commit();
                } catch (Exception e1) {
                }

              }
            }

          }

          this.getStatus().setExtendedMessage("Effected rows " + sjJob.getUpdateCount());
        }

        ResourcePool.releaseConnection(this.dbConnection);

        sjJob.getStatus().setStats(sjJob.getUpdateCount(), System.currentTimeMillis() - start);
        this.dbConnection = null;
      } catch (Exception e) {
        jsJobStatus.setErrorCode(2); // BRIAN: NEED TO SET UP SQL JOB ERROR CODES

        String msg;
        if (statement == -1) {
          msg = "Error running SQL: ";
        } else {
          msg = "Error running SQL Statement: " + statement;
        }

        if (curSQL != null)
          msg += "\n" + curSQL;

        while (e != null) {
          msg += "\t" + e.getMessage() + "\n";
          if (e instanceof SQLException) {
            if (e != ((SQLException) e).getNextException())
              e = ((SQLException) e).getNextException();
          } else
            e = null;
        }

        jsJobStatus.setErrorMessage(msg);
        if (this.dbConnection != null) {
          try {
            if (this.dbConnection.getAutoCommit() == false) {
              ResourcePool.LogMessage(this, ResourcePool.INFO_MESSAGE,
                  "Rolling back transaction due to error");
              this.dbConnection.rollback();
            }
          } catch (SQLException e1) {
            ResourcePool.LogMessage(this, ResourcePool.ERROR_MESSAGE, "Rollback possibly failed, "
                + e1.getMessage());
          }
          ResourcePool.releaseConnection(this.dbConnection);
          this.dbConnection = null;
        }

        return false;
      }
    } finally {
      this.sjJob = null;
      this.monitor.stmt = null;
      this.monitor.alive = false;
    }

    return true;
  }

  /**
   * Insert the method's description here. Creation date: (5/7/2002 11:55:23 AM)
   * 
   * @return true, if initialize
   */
  @Override
  protected boolean initialize() {
    // No need to do anything here.
    return true;
  }

  /**
   * Insert the method's description here. Creation date: (5/8/2002 2:52:39 PM)
   * 
   * @param jJob com.kni.etl.ETLJob
   * @return boolean
   */
  @Override
  public boolean supportsJobType(ETLJob jJob) {

    // Only accept SQL jobs...
    return this.isValidType(jJob) && (jJob instanceof SQLJob);
  }

  /**
   * Insert the method's description here. Creation date: (5/7/2002 11:56:15 AM)
   * 
   * @return true, if terminate
   */
  @Override
  protected boolean terminate() {
    // release our database connections...
    if (this.dbConnection != null) {
      return ResourcePool.releaseConnection(this.dbConnection);
    }

    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.kni.etl.ETLJobExecutor#getNewJob()
   */
  @Override
  public ETLJob getNewJob() throws Exception {
    return new SQLJob();
  }

  /**
   * The main method.
   * 
   * @param args the arguments
   */
  public static void main(String[] args) {
    String ketldir = System.getenv("KETLDIR");
    if (ketldir == null) {
      ResourcePool.LogMessage(Thread.currentThread(), ResourcePool.WARNING_MESSAGE,
          "KETLDIR not set, defaulting to working dir");
      ketldir = ".";
    }

    ExternalJarLoader.loadJars(new File(ketldir + File.separator + "conf" + File.separator
        + "Extra.Libraries"), "ketlextralibs", ";");

    ETLJobExecutor.execute(args, new SQLJobExecutor(), true);
  }

  @Override
  public ETLJob getCurrentETLJob() {
    return this.sjJob;
  }
}
