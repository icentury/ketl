/* If you have a scenario with daily and hourly jobs and want to guarantee that hourly jobs
 * get a thread when daily jobs are running, add the following 3 entries. Then use the new jobtype
 * KETLHOURLY
 * 
 * In addition you will need to add an entry to the server_executor table with the number of thread to be 
 * dedicated
 * 
 * See example customJobTypes for an example of how to use a new JOB_TYPE
 * Caution: The description column value is equal to JOB_TYPE do not change the values for default job types.
 */ 

 
INSERT INTO JOB_TYPE ( JOB_TYPE_ID, DESCRIPTION, CLASS_NAME ) VALUES ( 
30, 'KETLHOURLY', 'com.kni.etl.ketl.KETLJob'); 

INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
30, 'com.kni.etl.ketl.KETLJobExecutor'); 

INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
30, 30); 

/* Edit server id, see server table */
INSERT INTO SERVER_EXECUTOR (SERVER_ID, JOB_EXECUTOR_ID,THREADS ) VALUES ( :SERVERID,30, 2);

