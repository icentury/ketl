-- these should be exec with the ketlmd user:
ALTER TABLE JOB_LOG_HIST
ADD LAST_UPDATE_DATE	timestamp without time zone;
UPDATE JOB_LOG_HIST 
SET LAST_UPDATE_DATE = current_timestamp;

ALTER TABLE JOB_LOG
ADD LAST_UPDATE_DATE	timestamp without time zone;
UPDATE JOB_LOG
SET LAST_UPDATE_DATE = current_timestamp;

ALTER TABLE JOB
ADD LAST_UPDATE_DATE	timestamp without time zone;
UPDATE JOB 
SET LAST_UPDATE_DATE = current_timestamp;

ALTER TABLE JOB_SCHEDULE
ADD START_RUN_DATE  timestamp without time zone,
ADD STOP_RUN_DATE   timestamp without time zone;

INSERT INTO JOB_STATUS ( STATUS_ID, STATUS_DESC ) VALUES (
'10', 'Cancelled');
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

---------------------------------------------------------------
-- if this language has not been defined already,
-- this must be exec with the superuser id.
-- correct the path as needed.
--CREATE FUNCTION plpgsql_call_handler()
--         RETURNS OPAQUE AS '../lib/plpgsql.dll' LANGUAGE 'C';
--CREATE LANGUAGE 'plpgsql' HANDLER plpgsql_call_handler
--                            LANCOMPILER 'PL/pgSQL';        
---------------------------------------------------------------

-- these should be exec with the ketlmd user:
CREATE OR REPLACE FUNCTION moddate()
  RETURNS "trigger" AS
$BODY$
     BEGIN
         -- Check that empname and salary are given
         NEW.last_update_date  := current_timestamp;
         RETURN NEW;
     END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
  
ALTER FUNCTION moddate() OWNER TO ketlmd;

CREATE TRIGGER moddate
  BEFORE INSERT OR UPDATE
  ON job
  FOR EACH ROW
  EXECUTE PROCEDURE moddate();
  
CREATE TRIGGER moddate
  BEFORE INSERT OR UPDATE
  ON job_log
  FOR EACH ROW
  EXECUTE PROCEDURE moddate();
  
CREATE TRIGGER moddate
  BEFORE INSERT OR UPDATE
  ON job_log_hist
  FOR EACH ROW
  EXECUTE PROCEDURE moddate();
 