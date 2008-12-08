DROP TABLE server_executor;
DROP TABLE server;
DROP TABLE server_status;
DROP TABLE mail_server_detail;
DROP TABLE job_dependencie;
DROP TABLE job_schedule;
DROP TABLE job_error_hist;
DROP TABLE job_log_hist;
DROP TABLE job_error;
DROP TABLE job_log;
DROP TABLE load;
DROP TABLE job_executor_job_type;
DROP TABLE job_executor;
DROP TABLE alert_subscription;
DROP TABLE job;
DROP TABLE job_type;
DROP TABLE job_status;
DROP TABLE job_qa_hist;
DROP TABLE project;
DROP TABLE parameter;
DROP TABLE parameter_list;
DROP TABLE id_generator;
DROP TABLE alert_address;


CREATE TABLE alert_address (
       address_id           NUMERIC NOT NULL,     
       address_name         VARCHAR(50),
       address              VARCHAR(50),
       max_message_length   NUMERIC,
CONSTRAINT alert_address_pkey PRIMARY KEY (address_id)
);

CREATE TABLE id_generator (
       id_name              VARCHAR(50),
       start_value          NUMERIC,
       current_value        NUMERIC,
CONSTRAINT id_generator_pkey PRIMARY KEY (id_name)
);

CREATE TABLE parameter_list (
       parameter_list_id    NUMERIC NOT NULL,
       parameter_list_name  VARCHAR(255) NOT NULL,
CONSTRAINT parameter_list_pkey PRIMARY KEY (parameter_list_id)
);

CREATE TABLE parameter (
       parameter_id         NUMERIC NOT NULL,
       parameter_list_id    NUMERIC,
       parameter_name       VARCHAR(50),
       parameter_value      VARCHAR(4000),
       sub_parameter_list_id NUMERIC,
       sub_parameter_list_name VARCHAR(50),
CONSTRAINT parameter_parameter_list_fk FOREIGN KEY (parameter_list_id)
       REFERENCES parameter_list (parameter_list_id),
CONSTRAINT parameter_pkey PRIMARY KEY (parameter_id)
);

CREATE TABLE project (
       project_id           NUMERIC NOT NULL,
       project_desc         VARCHAR(255),
CONSTRAINT project_pkey PRIMARY KEY (project_id)
);

CREATE TABLE job_qa_hist (
       job_id               VARCHAR(60) NOT NULL,
       qa_id                VARCHAR(255) NOT NULL,
       qa_type              VARCHAR(28) NOT NULL,
       step_name            VARCHAR(255) NOT NULL,
       details              VARCHAR(4000) NOT NULL,
       record_date          TIMESTAMP NOT NULL,
CONSTRAINT job_qa_hist_pkey PRIMARY KEY (job_id, qa_id, qa_type, step_name, record_date)
);

CREATE TABLE job_status (
       status_id            NUMERIC NOT NULL,
       status_desc          VARCHAR(30),
CONSTRAINT job_status_pkey PRIMARY KEY (status_id)
);

CREATE TABLE job_type (
       job_type_id          NUMERIC NOT NULL,
       description          VARCHAR(20),
       class_name           VARCHAR(50),
CONSTRAINT job_type_pkey PRIMARY KEY (job_type_id)
);


CREATE TABLE job (
       job_id               VARCHAR(60) NOT NULL,
       job_type_id          NUMERIC NOT NULL,
       parameter_list_id    NUMERIC,
       project_id           NUMERIC,
       action               VARCHAR(32000),
       name                 VARCHAR(100),
       description          VARCHAR(4000),
       retry_attempts       NUMERIC,
       seconds_before_retry NUMERIC,
       disable_alerting     VARCHAR(1),
       last_update_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT job_parameter_list_fk FOREIGN KEY (parameter_list_id)
       REFERENCES parameter_list (parameter_list_id),
CONSTRAINT job_project_fk FOREIGN KEY (project_id)
       REFERENCES project (project_id),
CONSTRAINT job_job_type_fk FOREIGN KEY (job_type_id)
       REFERENCES job_type (job_type_id),
CONSTRAINT job_pkey PRIMARY KEY (job_id)
);

CREATE TABLE alert_subscription (
       project_id           NUMERIC,
       job_id               VARCHAR(60),
       address_id           NUMERIC,
       all_errors           VARCHAR(1),
       subject_prefix       VARCHAR(55),
CONSTRAINT alert_subs_project_fk FOREIGN KEY (project_id)
       REFERENCES project (project_id),
CONSTRAINT alert_subs_job_fk FOREIGN KEY (job_id)
       REFERENCES job (job_id),
CONSTRAINT alert_subs_alert_address_fk FOREIGN KEY (address_id)
       REFERENCES alert_address (address_id)
);

CREATE TABLE job_executor (
       job_executor_id      NUMERIC NOT NULL,
       class_name           VARCHAR(50),
CONSTRAINT job_executor_pkey PRIMARY KEY (job_executor_id)
);

CREATE TABLE job_executor_job_type (
       job_executor_id      NUMERIC NOT NULL,
       job_type_id          NUMERIC NOT NULL,
CONSTRAINT job_exec_job_type_fk FOREIGN KEY (job_type_id)
       REFERENCES job_type (job_type_id),
CONSTRAINT job_exec_executor_fk FOREIGN KEY (job_executor_id)
       REFERENCES job_executor (job_executor_id),
CONSTRAINT job_executor_job_type_pkey PRIMARY KEY (job_executor_id, job_type_id)
);

CREATE TABLE load (
       load_id              NUMERIC NOT NULL,
       start_job_id         VARCHAR(60),
       start_date           TIMESTAMP,
       project_id           NUMERIC,
       end_date             TIMESTAMP,
       ignored_parents      CHAR(1),
       failed               CHAR(1),
CONSTRAINT load_job_fk FOREIGN KEY (start_job_id)
       REFERENCES job  (job_id),
CONSTRAINT load_project_fk FOREIGN KEY (project_id)
       REFERENCES project  (project_id),
CONSTRAINT load_pkey PRIMARY KEY (load_id)
);

CREATE TABLE job_log (
       job_id               VARCHAR(60) NOT NULL,
       load_id              NUMERIC NOT NULL,
       start_date           TIMESTAMP,
       status_id            NUMERIC NOT NULL,
       end_date             TIMESTAMP,
       message              VARCHAR(2000),
       dm_load_id           NUMERIC NOT NULL,
       retry_attempts       NUMERIC,
       execution_date       TIMESTAMP,
       server_id            NUMERIC,
	   stats				TEXT,
       last_update_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT job_log_job_status_fk FOREIGN KEY (status_id)
       REFERENCES job_status  (status_id),
CONSTRAINT job_log_load_fk FOREIGN KEY (load_id)
       REFERENCES load  (load_id),
CONSTRAINT job_log_job_fk FOREIGN KEY (job_id)
       REFERENCES job  (job_id),
CONSTRAINT job_log_job_load_idx 
       UNIQUE (job_id,load_id),
CONSTRAINT job_log_pkey PRIMARY KEY (dm_load_id)
) ;

CREATE TABLE job_error (
       dm_load_id           NUMERIC NOT NULL,
       job_id               VARCHAR(60),
       message              VARCHAR(1000),
       code                 VARCHAR(20),
       error_datetime       TIMESTAMP,
CONSTRAINT job_error_job_fk FOREIGN KEY (job_id)
       REFERENCES job  (job_id),
CONSTRAINT job_error_job_log_fk FOREIGN KEY (dm_load_id)
       REFERENCES job_log  (dm_load_id)
);

CREATE TABLE job_log_hist (
       load_id              NUMERIC NOT NULL,
       start_date           TIMESTAMP,
       status_id            NUMERIC,
       end_date             TIMESTAMP,
       message              VARCHAR(2000),
       dm_load_id           NUMERIC NOT NULL,
       retry_attempts       NUMERIC,
       execution_date       TIMESTAMP,
       server_id            NUMERIC,
       job_id               VARCHAR(60) NOT NULL,
	   stats				TEXT,
       last_update_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT job_log_hist_job_fk FOREIGN KEY (job_id)
       REFERENCES job  (job_id),
CONSTRAINT job_log_hist_job_load_idx 
       UNIQUE (job_id,load_id),
CONSTRAINT job_log_hist_load_idx 
       UNIQUE (dm_load_id),
CONSTRAINT job_log_hist_pkey PRIMARY KEY (dm_load_id)
);

CREATE TABLE job_error_hist (
       dm_load_id           NUMERIC NOT NULL,
       message              VARCHAR(1000),
       code                 VARCHAR(20),
       error_datetime       TIMESTAMP,
       job_id               VARCHAR(60),
       details              VARCHAR(4000),
       step_name            VARCHAR(255),
CONSTRAINT job_error_hist_job_fk FOREIGN KEY (job_id)
       REFERENCES job  (job_id)
);

CREATE TABLE job_schedule (
       schedule_id          NUMERIC NOT NULL,
       job_id               VARCHAR(60) NOT NULL,
       month                NUMERIC,
       month_of_year        NUMERIC,
       day                  NUMERIC,
       day_of_week          NUMERIC,
       day_of_month         NUMERIC,
       hour_of_day          NUMERIC,
       hour                 NUMERIC,
       next_run_date        TIMESTAMP,
       schedule_desc        VARCHAR(255),
       minute_of_hour       NUMERIC,
       minute               NUMERIC,
       start_run_date       TIMESTAMP,
       stop_run_date        TIMESTAMP,
CONSTRAINT job_schedule_job_fk FOREIGN KEY (job_id)
       REFERENCES job  (job_id),
CONSTRAINT job_schedule_pkey PRIMARY KEY (schedule_id)
);

CREATE TABLE job_dependencie (
       job_id               VARCHAR(60) NOT NULL,
       parent_job_id        VARCHAR(60) NOT NULL,
       continue_if_failed   CHAR(1) NOT NULL,
CONSTRAINT  job_dependencie_job_parent_fk FOREIGN KEY (parent_job_id)
       REFERENCES job (job_id),
CONSTRAINT job_dependencie_job_fk FOREIGN KEY (job_id)
       REFERENCES job  (job_id),
CONSTRAINT parent_job_id_job_id_idx
       UNIQUE (parent_job_id,job_id ),
CONSTRAINT job_dependencie_pkey PRIMARY KEY (job_id, parent_job_id)
);

CREATE TABLE mail_server_detail (
       mailserver_id        NUMERIC NOT NULL,
       hostname             VARCHAR(256),
       login                VARCHAR(256),
       pwd                  VARCHAR(256),
       from_address         VARCHAR(50),
CONSTRAINT mail_server_detail_pkey PRIMARY KEY (mailserver_id)
);



CREATE TABLE server_status (
       status_id            NUMERIC NOT NULL,
       status_desc          VARCHAR(50) NOT NULL,
CONSTRAINT server_status_pkey PRIMARY KEY (status_id)
);


CREATE TABLE server (
       server_id            NUMERIC NOT NULL,
       server_name          VARCHAR(20) NOT NULL,
       status_id            NUMERIC NOT NULL,
       shutdown_now         VARCHAR(20),
       start_time           TIMESTAMP,
       shutdown_time        TIMESTAMP,
       last_ping_time       TIMESTAMP,
CONSTRAINT server_server_status_fk FOREIGN KEY (status_id)
       REFERENCES server_status  (status_id),
CONSTRAINT server_pkey PRIMARY KEY (server_id)
);

CREATE TABLE server_executor (
       server_id            NUMERIC NOT NULL,
       job_executor_id      NUMERIC NOT NULL,
       threads              NUMERIC,
       queue_size           NUMERIC,
CONSTRAINT server_exec_job_exec_fk FOREIGN KEY (job_executor_id)
       REFERENCES job_executor (job_executor_id),
CONSTRAINT server_exec_server_fk FOREIGN KEY (server_id)
       REFERENCES server (server_id),
CONSTRAINT server_exec_pkey PRIMARY KEY (server_id, job_executor_id) 
);






--DROP VIEW job_progress;

--DROP VIEW executing_and_complete_jobs;

--DROP VIEW executing_and_complete_jobhist;

--CREATE VIEW job_progress(
--                        status, 
--                        "run time", 
--                       "start delay", 
--                      job_id, 
--                      execution_date, 
--                        end_date, 
--                        status_id, 
--                        message, 
--                        dm_load_id, 
--                        load_id, 
--                        start_date, 
--                        server_id, 
--                        retry_attempts, 
--                        job_type_id, 
--                        project_id, 
--                        name, 
--                        description, 
--                        action)
--AS 
--SELECT (CASE job_log.status_id
--	   		 WHEN 1 THEN 'A: Running'
--	 WHEN 2 THEN 'B: Error'
--  		 WHEN 3 THEN 'F: Finished'
--			 WHEN 4 THEN 'C: Ready to run'
--			 WHEN 12 THEN 'D:  Waiting to retry'
--			 WHEN 5 THEN 'E: Waiting for children'
--			 WHEN 6 THEN 'B: Error'
--			 WHEN 7 THEN 'F: Finished'
--			 ELSE 'G: Not in project' END) AS status,
 --                       (COALESCE(job_log.end_date,current_timestamp) - job_log.execution_date) AS "Run Time",
  --                      (COALESCE(job_log.execution_date,current_timestamp) - job_log.start_date) AS "Start Delay",
   --                     job.job_id,
    --                    job_log.execution_date, 
     --                   job_log.end_date, 
--                        job_log.status_id, 
--                        job_log.message, 
--                        job_log.dm_load_id,
--                        job_log.load_id, 
--                        job_log.start_date,
--                        job_log.server_id, 
--                        job_log.retry_attempts, 
--                       job.job_type_id, 
--                        job.project_id,
--                        job.name, 
--                        job.description, 
--                        job.action
--FROM job left outer join job_log ON job.job_id = job_log.job_id
----where (coalesce(log.end_date,current_timestamp) - log.execution_date)*24 > 0.24999
--ORDER BY                1, 
--                       COALESCE(end_date,current_timestamp) DESC, 
--                        "run time" DESC,
--                        job_log.dm_load_id, 
--                        job.job_id;
--
--CREATE VIEW executing_and_complete_jobs (load_id, load_start, load_end, job_id, execution_time_in_minutes, job_submitted_at, execution_started, execution_finished, executing_server, message)  AS
--       SELECT c.load_id, c.start_date, c.end_date, job_id, round(((nvl(a.end_date, sysdate) - execution_date) * 24 * 60), 2), a.start_date, a.execution_date, a.end_date, server_name, a.message
--       FROM load c, server b, job_log a
--       WHERE execution_date IS NOT NULL AND a.server_id = b.server_id AND c.load_id = a.load_id;

--CREATE VIEW executing_and_complete_jobhist (load_id, load_start, load_end, job_id, execution_time_in_minutes, job_submitted_at, execution_started, execution_finished, executing_server, message)  AS
--       SELECT c.load_id, c.start_date, c.end_date, substr(job_id, 0, length(job_id)), round(((a.end_date-execution_date) * 24 * 60), 2), a.start_date, a.execution_date, a.end_date, substr(server_name, 0, length(server_name)), a.message
--       FROM load c, server b, job_log_hist a
--       WHERE execution_date IS NOT NULL AND a.server_id = b.server_id AND c.load_id = a.load_id;



----create sequences

CREATE SEQUENCE LOAD_ID
    START WITH 1
    INCREMENT BY 1;


CREATE SEQUENCE SERVER_ID
    START WITH 1
    INCREMENT BY 1;

-- SEED TABLES --

INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
1, 'com.kni.etl.SQLJobExecutor'); 
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
2, 'com.kni.etl.OSJobExecutor'); 
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
3, 'com.kni.etl.ketl.KETLJobExecutor'); 
INSERT INTO JOB_EXECUTOR ( JOB_EXECUTOR_ID, CLASS_NAME ) VALUES ( 
4, 'com.kni.etl.sessionizer.XMLSessionizeJobExecutor'); 
 
 
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


INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
1, 1); 
INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
2, 2); 
INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
3, 3); 
INSERT INTO JOB_EXECUTOR_JOB_TYPE ( JOB_EXECUTOR_ID, JOB_TYPE_ID ) VALUES ( 
4, 4);  
 
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


CREATE  TRIGGER T_JOB_MODDATE_UPDATE 
	BEFORE UPDATE ON JOB 
	FOR EACH ROW 
	CALL "com.kni.etl.H2Triggers.TJobModdate";

CREATE TRIGGER T_JOB_LOG_MODDATE 
	BEFORE UPDATE ON JOB_LOG
	FOR EACH ROW 
	CALL "com.kni.etl.H2Triggers.TJobModdate";

CREATE  TRIGGER T_JOB_LOG_HIST_MODDATE 
	BEFORE UPDATE ON JOB_LOG_HIST
	FOR EACH ROW
	CALL "com.kni.etl.H2Triggers.TJobModdate";

CHECKPOINT SYNC;
