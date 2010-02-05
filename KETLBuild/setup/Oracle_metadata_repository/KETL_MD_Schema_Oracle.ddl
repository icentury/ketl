/* Create seeded metadata tables */

CREATE TABLE ALERT_ADDRESS
(
  ADDRESS_ID          NUMBER                    NOT NULL,
  ADDRESS_NAME        VARCHAR2(50 BYTE),
  ADDRESS             VARCHAR2(50 BYTE),
  MAX_MESSAGE_LENGTH  NUMBER
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE ID_GENERATOR
(
  ID_NAME        VARCHAR2(50 BYTE),
  START_VALUE    NUMBER,
  CURRENT_VALUE  NUMBER
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_ERROR_HIST
(
  DM_LOAD_ID      NUMBER                        NOT NULL,
  JOB_ID          VARCHAR2(60 BYTE),
  MESSAGE         VARCHAR2(1000 BYTE),
  CODE            VARCHAR2(20 BYTE),
  ERROR_DATETIME  DATE,
  DETAILS         VARCHAR2(4000 BYTE),
  STEP_NAME       VARCHAR2(255 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_EXECUTOR
(
  JOB_EXECUTOR_ID  NUMBER,
  CLASS_NAME       VARCHAR2(50 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_LOG_HIST
(
  JOB_ID          VARCHAR2(60 BYTE)             NOT NULL,
  LOAD_ID         NUMBER                        NOT NULL,
  START_DATE      DATE,
  STATUS_ID       NUMBER,
  END_DATE        DATE,
  MESSAGE         VARCHAR2(2000 BYTE),
  DM_LOAD_ID      NUMBER                        NOT NULL,
  RETRY_ATTEMPTS  NUMBER,
  EXECUTION_DATE  DATE,
  SERVER_ID       NUMBER,
  LAST_UPDATE_DATE	DATE
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_QA_HIST
(
  JOB_ID       VARCHAR2(60 BYTE)                NOT NULL,
  QA_ID        VARCHAR2(255 BYTE)               NOT NULL,
  QA_TYPE      VARCHAR2(28 BYTE)                NOT NULL,
  STEP_NAME    VARCHAR2(255 BYTE)               NOT NULL,
  DETAILS      VARCHAR2(4000 BYTE)              NOT NULL,
  RECORD_DATE  DATE                             NOT NULL
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_STATUS
(
  STATUS_ID    NUMBER                    NOT NULL,
  STATUS_DESC  VARCHAR2(30 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_TYPE
(
  JOB_TYPE_ID  NUMBER                           NOT NULL,
  DESCRIPTION  VARCHAR2(20 BYTE),
  CLASS_NAME   VARCHAR2(50 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE MAIL_SERVER_DETAIL
(
  MAILSERVER_ID  NUMBER                         NOT NULL,
  HOSTNAME       VARCHAR2(256 BYTE),
  LOGIN          VARCHAR2(256 BYTE),
  PWD            VARCHAR2(256 BYTE),
  FROM_ADDRESS   VARCHAR2(50 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE PARAMETER_LIST
(
  PARAMETER_LIST_ID    NUMBER                   NOT NULL,
  PARAMETER_LIST_NAME  VARCHAR2(255 BYTE)       NOT NULL
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE PROJECT
(
  PROJECT_ID    NUMBER                          NOT NULL,
  PROJECT_DESC  VARCHAR2(255 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE SERVER_STATUS
(
  STATUS_ID    NUMBER                           NOT NULL,
  STATUS_DESC  VARCHAR2(50 BYTE)                NOT NULL
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE UNIQUE INDEX PK_JOB_QA_HISTORY ON JOB_QA_HIST
(JOB_ID, QA_ID, QA_TYPE, STEP_NAME, RECORD_DATE)
LOGGING
NOPARALLEL;


CREATE TABLE JOB
(
  JOB_ID                VARCHAR2(60 BYTE)       NOT NULL,
  JOB_TYPE_ID           NUMBER                  NOT NULL,
  PARAMETER_LIST_ID     NUMBER,
  PROJECT_ID            NUMBER,
  NAME                  VARCHAR2(100 BYTE),
  DESCRIPTION           VARCHAR2(4000 BYTE),
  RETRY_ATTEMPTS        NUMBER,
  SECONDS_BEFORE_RETRY  NUMBER,
  DISABLE_ALERTING      VARCHAR2(1 BYTE),
  ACTION                LONG,
  LAST_UPDATE_DATE	DATE,
  POOL					VARCHAR2(50)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_DEPENDENCIE
(
  JOB_ID              VARCHAR2(60 BYTE)         NOT NULL,
  PARENT_JOB_ID       VARCHAR2(60 BYTE)         NOT NULL,
  CONTINUE_IF_FAILED  CHAR(1 BYTE)              NOT NULL
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_EXECUTOR_JOB_TYPE
(
  JOB_EXECUTOR_ID  NUMBER                       NOT NULL,
  JOB_TYPE_ID      NUMBER                       NOT NULL
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_SCHEDULE
(
  SCHEDULE_ID     NUMBER                        NOT NULL,
  JOB_ID          VARCHAR2(60 BYTE)             NOT NULL,
  MONTH           NUMBER,
  MONTH_OF_YEAR   NUMBER,
  DAY             NUMBER,
  DAY_OF_WEEK     NUMBER,
  DAY_OF_MONTH    NUMBER,
  HOUR_OF_DAY     NUMBER,
  HOUR            NUMBER,
  NEXT_RUN_DATE   DATE,
  SCHEDULE_DESC   VARCHAR2(255 BYTE),
  MINUTE_OF_HOUR  NUMBER,
  MINUTE          NUMBER,
  START_RUN_DATE  DATE,
  STOP_RUN_DATE   DATE
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE LOAD
(
  LOAD_ID          NUMBER                       NOT NULL,
  START_JOB_ID     VARCHAR2(60 BYTE),
  START_DATE       DATE,
  PROJECT_ID       NUMBER,
  END_DATE         DATE,
  IGNORED_PARENTS  CHAR(1 BYTE),
  FAILED           CHAR(1 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE PARAMETER
(
  PARAMETER_ID             NUMBER               NOT NULL,
  PARAMETER_LIST_ID        NUMBER,
  PARAMETER_NAME           VARCHAR2(50 BYTE),
  PARAMETER_VALUE          VARCHAR2(4000 BYTE),
  SUB_PARAMETER_LIST_ID    NUMBER,
  SUB_PARAMETER_LIST_NAME  VARCHAR2(50 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE SERVER
(
  SERVER_ID       NUMBER,
  SERVER_NAME     VARCHAR2(256 BYTE)            NOT NULL,
  STATUS_ID       NUMBER                        NOT NULL,
  SHUTDOWN_NOW    CHAR(1 BYTE),
  START_TIME      DATE,
  SHUTDOWN_TIME   DATE,
  LAST_PING_TIME  DATE
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE SERVER_EXECUTOR
(
  SERVER_ID        NUMBER,
  JOB_EXECUTOR_ID  NUMBER,
  THREADS          NUMBER,
  QUEUE_SIZE       NUMBER,
  POOL			   VARCHAR2(50) 
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE BITMAP INDEX JOB_ID_DEP_IDX ON JOB_DEPENDENCIE
(JOB_ID)
LOGGING
NOPARALLEL;


CREATE UNIQUE INDEX OP_DEP_IDX ON JOB_DEPENDENCIE
(PARENT_JOB_ID, JOB_ID)
LOGGING
NOPARALLEL;


CREATE BITMAP INDEX PAR_JOB_ID_DEP_IDX ON JOB_DEPENDENCIE
(PARENT_JOB_ID)
LOGGING
NOPARALLEL;


CREATE TABLE ALERT_SUBSCRIPTION
(
  PROJECT_ID      NUMBER,
  JOB_ID          VARCHAR2(60 BYTE),
  ADDRESS_ID      NUMBER,
  ALL_ERRORS      VARCHAR2(1 BYTE),
  SUBJECT_PREFIX  VARCHAR2(55 BYTE)
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE TABLE JOB_LOG
(
  JOB_ID          VARCHAR2(60 BYTE)             NOT NULL,
  LOAD_ID         NUMBER                        NOT NULL,
  START_DATE      DATE,
  STATUS_ID       NUMBER,
  END_DATE        DATE,
  MESSAGE         VARCHAR2(2000 BYTE),
  DM_LOAD_ID      NUMBER                        NOT NULL,
  RETRY_ATTEMPTS  NUMBER,
  EXECUTION_DATE  DATE,
  SERVER_ID       NUMBER,
  LAST_UPDATE_DATE	DATE
)
LOGGING 
NOCACHE
NOPARALLEL;


CREATE BITMAP INDEX XIE1JOB_LOG ON JOB_LOG
(STATUS_ID)
LOGGING
NOPARALLEL;


CREATE TABLE JOB_ERROR
(
  DM_LOAD_ID      NUMBER                        NOT NULL,
  JOB_ID          VARCHAR2(60 BYTE),
  MESSAGE         VARCHAR2(1000 BYTE),
  CODE            VARCHAR2(20 BYTE),
  ERROR_DATETIME  DATE
)
LOGGING 
NOCACHE
NOPARALLEL;


ALTER TABLE ALERT_ADDRESS ADD (
  PRIMARY KEY (ADDRESS_ID));


ALTER TABLE JOB_EXECUTOR ADD (
  PRIMARY KEY (JOB_EXECUTOR_ID));


ALTER TABLE JOB_LOG_HIST ADD (
  PRIMARY KEY (DM_LOAD_ID));

ALTER TABLE JOB_LOG_HIST ADD (
  UNIQUE (JOB_ID, LOAD_ID));


ALTER TABLE JOB_QA_HIST ADD (
  CONSTRAINT PK_JOB_QA_HISTORY PRIMARY KEY (JOB_ID, QA_ID, QA_TYPE, STEP_NAME, RECORD_DATE));


ALTER TABLE JOB_STATUS ADD (
  PRIMARY KEY (STATUS_ID));


ALTER TABLE JOB_TYPE ADD (
  PRIMARY KEY (JOB_TYPE_ID));


ALTER TABLE MAIL_SERVER_DETAIL ADD (
  PRIMARY KEY (MAILSERVER_ID));


ALTER TABLE PARAMETER_LIST ADD (
  PRIMARY KEY (PARAMETER_LIST_ID));


ALTER TABLE PROJECT ADD (
  PRIMARY KEY (PROJECT_ID));


ALTER TABLE SERVER_STATUS ADD (
  PRIMARY KEY (STATUS_ID));


ALTER TABLE JOB ADD (
  PRIMARY KEY (JOB_ID));


ALTER TABLE JOB_DEPENDENCIE ADD (
  PRIMARY KEY (JOB_ID, PARENT_JOB_ID));


ALTER TABLE JOB_EXECUTOR_JOB_TYPE ADD (
  PRIMARY KEY (JOB_EXECUTOR_ID, JOB_TYPE_ID));


ALTER TABLE JOB_SCHEDULE ADD (
  PRIMARY KEY (SCHEDULE_ID));


ALTER TABLE LOAD ADD (
  PRIMARY KEY (LOAD_ID));


ALTER TABLE PARAMETER ADD (
  PRIMARY KEY (PARAMETER_ID));


ALTER TABLE SERVER ADD (
  PRIMARY KEY (SERVER_ID));


ALTER TABLE SERVER_EXECUTOR ADD (
  PRIMARY KEY (SERVER_ID, JOB_EXECUTOR_ID));


ALTER TABLE JOB_LOG ADD (
  PRIMARY KEY (DM_LOAD_ID));

ALTER TABLE JOB_LOG ADD (
  UNIQUE (JOB_ID, LOAD_ID));


ALTER TABLE JOB ADD (
  FOREIGN KEY (PARAMETER_LIST_ID) 
    REFERENCES PARAMETER_LIST (PARAMETER_LIST_ID));

ALTER TABLE JOB ADD (
  FOREIGN KEY (PROJECT_ID) 
    REFERENCES PROJECT (PROJECT_ID));

ALTER TABLE JOB ADD (
  FOREIGN KEY (JOB_TYPE_ID) 
    REFERENCES JOB_TYPE (JOB_TYPE_ID));


ALTER TABLE JOB_DEPENDENCIE ADD (
  FOREIGN KEY (PARENT_JOB_ID) 
    REFERENCES JOB (JOB_ID));

ALTER TABLE JOB_DEPENDENCIE ADD (
  FOREIGN KEY (JOB_ID) 
    REFERENCES JOB (JOB_ID));


ALTER TABLE JOB_EXECUTOR_JOB_TYPE ADD (
  FOREIGN KEY (JOB_TYPE_ID) 
    REFERENCES JOB_TYPE (JOB_TYPE_ID));

ALTER TABLE JOB_EXECUTOR_JOB_TYPE ADD (
  FOREIGN KEY (JOB_EXECUTOR_ID) 
    REFERENCES JOB_EXECUTOR (JOB_EXECUTOR_ID));


ALTER TABLE JOB_SCHEDULE ADD (
  FOREIGN KEY (JOB_ID) 
    REFERENCES JOB (JOB_ID));


ALTER TABLE LOAD ADD (
  FOREIGN KEY (START_JOB_ID) 
    REFERENCES JOB (JOB_ID) DISABLE);

ALTER TABLE LOAD ADD (
  FOREIGN KEY (PROJECT_ID) 
    REFERENCES PROJECT (PROJECT_ID) DISABLE);


ALTER TABLE PARAMETER ADD (
  FOREIGN KEY (PARAMETER_LIST_ID) 
    REFERENCES PARAMETER_LIST (PARAMETER_LIST_ID));


ALTER TABLE SERVER ADD (
  FOREIGN KEY (STATUS_ID) 
    REFERENCES SERVER_STATUS (STATUS_ID));


ALTER TABLE SERVER_EXECUTOR ADD (
  FOREIGN KEY (JOB_EXECUTOR_ID) 
    REFERENCES JOB_EXECUTOR (JOB_EXECUTOR_ID));

ALTER TABLE SERVER_EXECUTOR ADD (
  FOREIGN KEY (SERVER_ID) 
    REFERENCES SERVER (SERVER_ID));


ALTER TABLE ALERT_SUBSCRIPTION ADD (
  FOREIGN KEY (PROJECT_ID) 
    REFERENCES PROJECT (PROJECT_ID));

ALTER TABLE ALERT_SUBSCRIPTION ADD (
  FOREIGN KEY (JOB_ID) 
    REFERENCES JOB (JOB_ID));

ALTER TABLE ALERT_SUBSCRIPTION ADD (
  FOREIGN KEY (ADDRESS_ID) 
    REFERENCES ALERT_ADDRESS (ADDRESS_ID));


ALTER TABLE JOB_LOG ADD (
  FOREIGN KEY (STATUS_ID) 
    REFERENCES JOB_STATUS (STATUS_ID));

ALTER TABLE JOB_LOG ADD (
  FOREIGN KEY (LOAD_ID) 
    REFERENCES LOAD (LOAD_ID));

ALTER TABLE JOB_LOG ADD (
  FOREIGN KEY (JOB_ID) 
    REFERENCES JOB (JOB_ID));


ALTER TABLE JOB_ERROR ADD (
  FOREIGN KEY (JOB_ID) 
    REFERENCES JOB (JOB_ID));

ALTER TABLE JOB_ERROR ADD (
  FOREIGN KEY (DM_LOAD_ID) 
    REFERENCES JOB_LOG (DM_LOAD_ID));
	
/* create sequences */

CREATE SEQUENCE LOAD_ID
  START WITH 1
  MAXVALUE 1E27
  MINVALUE 0
  NOCYCLE
  NOCACHE
  NOORDER;


CREATE SEQUENCE SERVER_ID
  START WITH 1
  MAXVALUE 1E27
  MINVALUE 0
  NOCYCLE
  NOCACHE
  NOORDER;
  	

/* seed tables */
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
1, 'com.kni.etl.SQLJobExecutor'); 
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
2, 'com.kni.etl.OSJobExecutor'); 
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
3, 'com.kni.etl.ketl.KETLJobExecutor'); 
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
4, 'com.kni.etl.sessionizer.XMLSessionizeJobExecutor'); 
commit;
 
 
INSERT INTO JOB_TYPE ( JOB_TYPE_ID, DESCRIPTION, CLASS_NAME ) VALUES ( 
0, 'EMPTYJOB', NULL); 
INSERT INTO JOB_TYPE ( JOB_TYPE_ID, DESCRIPTION, CLASS_NAME ) VALUES ( 
1, 'SQL', 'com.kni.etl.SQLJob'); 
INSERT INTO JOB_TYPE ( JOB_TYPE_ID, DESCRIPTION, CLASS_NAME ) VALUES ( 
2, 'OSJOB', 'com.kni.etl.OSJob'); 
INSERT INTO JOB_TYPE ( JOB_TYPE_ID, DESCRIPTION, CLASS_NAME ) VALUES ( 
3, 'KETL', 'com.kni.etl.ketl.KETLJob'); 
INSERT INTO JOB_TYPE ( JOB_TYPE_ID, DESCRIPTION, CLASS_NAME ) VALUES ( 
4, 'XMLSESSIONIZER', 'com.kni.etl.sessionizer.XMLSessionizeJob'); 
commit;


INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
1, 1); 
INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
2, 2); 
INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
3, 3); 
INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
4, 4);  
commit;
 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'14', 'Critical Failure Pause Load'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'1', 'Executing'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'2', 'Pending Closure Failed'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'3', 'Finished'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'4', 'Waiting to be execut'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'5', 'Waiting for children'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'6', 'Failed'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'7', 'Pending Closure Finished'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'9', 'Pending Closure Cancelled'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'10', 'Cancelled'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'12', 'Waiting to be retried'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'15', 'Paused'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'16', 'Waiting to pause'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'17', 'Waiting to skip'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'18', 'Attempt pause'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'19', 'Resume'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'20', 'Pending Closure Skip'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'21', 'Skipped'); 
INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
'22', 'Attempt cancel'); 
commit;
 

 
INSERT INTO SERVER_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
1, 'Active'); 
INSERT INTO SERVER_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
2, 'Shutting Down'); 
INSERT INTO SERVER_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
3, 'Shutdown'); 
INSERT INTO SERVER_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
4, 'Paused'); 
INSERT INTO SERVER_STATUS ( STATUS_ID, STATUS_DESC ) VALUES ( 
5, 'Server Killed'); 
commit;


CREATE OR REPLACE FORCE VIEW JOB_PROGRESS
(STATUS, "Run Time", "Start Delay", JOB_ID, EXECUTION_DATE, 
 END_DATE, STATUS_ID, MESSAGE, DM_LOAD_ID, LOAD_ID, 
 START_DATE, SERVER_ID, RETRY_ATTEMPTS, JOB_TYPE_ID, PROJECT_ID, 
 NAME, DESCRIPTION, ACTION)
AS 
SELECT (CASE LOG.status_id
	   		 WHEN 1 THEN 'A: Running'
			 WHEN 2 THEN 'B: Error'
	   		 WHEN 3 THEN 'F: Finished'
			 WHEN 4 THEN 'C: Ready to run'
			 WHEN 12 THEN 'D:  Waiting to retry'
			 WHEN 5 THEN 'E: Waiting for children'
			 WHEN 6 THEN 'B: Error'
			 WHEN 7 THEN 'F: Finished'
			 ELSE 'G: Not in project' END) AS Status,
	   (COALESCE(LOG.end_date,current_timestamp) - LOG.execution_date) AS "Run Time",
	   (COALESCE(LOG.execution_date,current_timestamp) - LOG.start_date) AS "Start Delay",
	   JOB.job_id,
	   LOG.execution_date, LOG.end_date, LOG.status_id, LOG.message, LOG.dm_load_id, LOG.load_id, LOG.start_date,LOG.server_id, LOG.retry_attempts, JOB.job_type_id, JOB.project_id, JOB.name, JOB.description, JOB.action
FROM JOB left join JOB_LOG LOG ON JOB.job_id = LOG.job_id
--where (coalesce(log.end_date,current_timestamp) - log.execution_date)*24 > 0.24999
ORDER BY Status, COALESCE(end_date,current_timestamp) DESC, "Run Time" DESC,LOG.dm_load_id, JOB.job_id;


 
	
CREATE OR REPLACE FORCE VIEW EXECUTING_AND_COMPLETE_JOBS
(LOAD_ID, LOAD_START, LOAD_END, JOB_ID, EXECUTION_TIME_IN_MINUTES, 
 JOB_SUBMITTED_AT, EXECUTION_STARTED, EXECUTION_FINISHED, EXECUTING_SERVER, MESSAGE)
AS 
select c.load_id,c.start_date load_start,c.end_date load_end,substr(job_id,0,length(job_id)) Job_ID,
       round(((nvl(a.end_date,sysdate)-execution_date)*24*60),2) Execution_Time_In_Minutes,
           a.start_Date Job_Submitted_At,(execution_Date) Execution_Started,
           (a.end_Date) Execution_Finished,
           substr(server_name,0,length(server_name)) Executing_Server,
           message Message
 from job_log a, server b, load c
where execution_date is not null
  and a.server_id = b.server_id
  and c.load_id = a.load_id;


CREATE OR REPLACE FORCE VIEW EXECUTING_AND_COMPLETE_JOBHIST
(LOAD_ID, LOAD_START, LOAD_END, JOB_ID, EXECUTION_TIME_IN_MINUTES, 
 JOB_SUBMITTED_AT, EXECUTION_STARTED, EXECUTION_FINISHED, EXECUTING_SERVER, MESSAGE)
AS 
select c.load_id,c.start_date load_start,c.end_date load_end,substr(job_id,0,length(job_id)) Job_ID,
       round(((a.end_date-execution_date)*24*60),2) Execution_Time_In_Minutes,
           a.start_Date Job_Submitted_At,(execution_Date) Execution_Started,
           (a.end_Date) Execution_Finished,
           substr(server_name,0,length(server_name)) Executing_Server,
           message Message
 from job_log_HIST a, server b, load c
where execution_date is not null
  and a.server_id = b.server_id
  and c.load_id = a.load_id;


	

CREATE OR REPLACE TRIGGER T_JOB_LOG_MODDATE 
	BEFORE INSERT OR UPDATE ON JOB_LOG
	FOR EACH ROW 
	BEGIN 
		:NEW.LAST_UPDATE_DATE := SYSDATE;
	END;
.
run;

CREATE OR REPLACE TRIGGER T_JOB_LOG_HIST_MODDATE 
	BEFORE INSERT OR UPDATE ON JOB_LOG_HIST
	FOR EACH ROW 
	BEGIN 
		:NEW.LAST_UPDATE_DATE := SYSDATE;
	END;
.
run;

