-- Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
-- PostgreSQL database dump
--

SET client_encoding = 'UNICODE';
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 1874 (class 0 OID 0)
-- Name: DUMP TIMESTAMP; Type: DUMP TIMESTAMP; Schema: -; Owner: 
--

-- Started on 2005-07-05 14:01:10 Pacific Standard Time


--
-- TOC entry 9 (class 16672 OID 205753145)
-- Name: ketlmd; Type: SCHEMA; Schema: -; Owner: ketlmd
--

CREATE SCHEMA ketlmd;


ALTER SCHEMA ketlmd OWNER TO ketlmd;

SET search_path = ketlmd, pg_catalog;

SET default_with_oids = true;

--
-- TOC entry 1443 (class 1259 OID 205753146)
-- Dependencies: 9
-- Name: alert_address; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE alert_address (
    address_id integer NOT NULL,
    address_name character varying(50),
    address character varying(50),
    max_message_length smallint
);


ALTER TABLE alert_address OWNER TO ketlmd;

--
-- TOC entry 1463 (class 1259 OID 205753208)
-- Dependencies: 9
-- Name: alert_subscription; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE alert_subscription (
    project_id integer,
    job_id character varying(60),
    address_id integer,
    all_errors character varying(1),
    subject_prefix character varying(55)
);


ALTER TABLE alert_subscription OWNER TO ketlmd;

--
-- TOC entry 1444 (class 1259 OID 205753148)
-- Dependencies: 9
-- Name: id_generator; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE id_generator (
    id_name character varying(50),
    start_value bigint,
    current_value bigint
);


ALTER TABLE id_generator OWNER TO ketlmd;

--
-- TOC entry 1455 (class 1259 OID 205753183)
-- Dependencies: 9
-- Name: job; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job (
    job_id character varying(60) NOT NULL,
    job_type_id integer NOT NULL,
    parameter_list_id integer,
    project_id integer,
    name character varying(100),
    description character varying(4000),
    retry_attempts integer,
    seconds_before_retry integer,
    disable_alerting character varying(1),
    "action" text,
    old_action2 bytea,
    last_update_date timestamp without time zone,
	pool text,
    priority integer 
);


ALTER TABLE job OWNER TO ketlmd;

--
-- TOC entry 1456 (class 1259 OID 205753188)
-- Dependencies: 9
-- Name: job_dependencie; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_dependencie (
    job_id character varying(60) NOT NULL,
    parent_job_id character varying(60) NOT NULL,
    continue_if_failed character(1) NOT NULL
);


ALTER TABLE job_dependencie OWNER TO ketlmd;

--
-- TOC entry 1465 (class 1259 OID 205753216)
-- Dependencies: 9
-- Name: job_error; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_error (
    dm_load_id integer NOT NULL,
    job_id character varying(60),
    message character varying(1000),
    code character varying(20),
    error_datetime timestamp without time zone
);


ALTER TABLE job_error OWNER TO ketlmd;

--
-- TOC entry 1445 (class 1259 OID 205753150)
-- Dependencies: 9
-- Name: job_error_hist; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_error_hist (
    dm_load_id integer NOT NULL,
    job_id character varying(60),
    message character varying(1000),
    code character varying(20),
    error_datetime timestamp without time zone,
    details character varying(4000),
    step_name character varying(255)
);


ALTER TABLE job_error_hist OWNER TO ketlmd;

--
-- TOC entry 1446 (class 1259 OID 205753155)
-- Dependencies: 9
-- Name: job_executor; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_executor (
    job_executor_id integer NOT NULL,
    class_name character varying(50)
);


ALTER TABLE job_executor OWNER TO ketlmd;

--
-- TOC entry 1457 (class 1259 OID 205753190)
-- Dependencies: 9
-- Name: job_executor_job_type; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_executor_job_type (
    job_executor_id integer NOT NULL,
    job_type_id integer NOT NULL
);


ALTER TABLE job_executor_job_type OWNER TO ketlmd;

--
-- TOC entry 1464 (class 1259 OID 205753210)
-- Dependencies: 9
-- Name: job_log; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_log (
    job_id character varying(60) NOT NULL,
    load_id integer NOT NULL,
    start_date timestamp without time zone,
    status_id int2,
    end_date timestamp without time zone,
    message character varying(2000),
    dm_load_id integer NOT NULL,
    retry_attempts integer,
    execution_date timestamp without time zone,
    server_id integer,
    last_update_date timestamp without time zone
);


ALTER TABLE job_log OWNER TO ketlmd;




--
-- TOC entry 1447 (class 1259 OID 205753157)
-- Dependencies: 9
-- Name: job_log_hist; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_log_hist (
    job_id character varying(60) NOT NULL,
    load_id integer NOT NULL,
    start_date timestamp without time zone,
    status_id int2,
    end_date timestamp without time zone,
    message character varying(2000),
    dm_load_id integer NOT NULL,
    retry_attempts integer,
    execution_date timestamp without time zone,
    server_id integer,
    last_update_date timestamp without time zone
);


ALTER TABLE job_log_hist OWNER TO ketlmd;

--
-- TOC entry 1468 (class 1259 OID 205753387)
-- Dependencies: 1552 9
-- Name: job_progress; Type: VIEW; Schema: ketlmd; Owner: ketlmd
--

CREATE VIEW job_progress AS
    SELECT CASE log.status_id 
                WHEN 1 THEN 'A: Running'::bpchar 
                WHEN 2 THEN 'B: Error'::bpchar 
	        WHEN 3 THEN 'F: Finished'::bpchar 
	        WHEN 4 THEN 'C: Ready to run'::bpchar 
	        WHEN 12 THEN 'D:  Waiting to retry'::bpchar 
	        WHEN 5 THEN 'E: Waiting for children'::bpchar 
	        WHEN 6 THEN 'B: Error'::bpchar 
	        WHEN 7 THEN 'F: Finished'::bpchar 
	        ELSE 'G: Not in project' END AS status, 
           (COALESCE((log.end_date)::timestamp with time zone, ('now'::text)::timestamp(6) with time zone) - (log.execution_date)::timestamp with time zone) AS "Run Time", 
           (COALESCE((log.execution_date)::timestamp with time zone, ('now'::text)::timestamp(6) with time zone) - (log.start_date)::timestamp with time zone) AS "Start Delay", 
           job.job_id, log.execution_date AS execution_timestamp, log.end_date AS end_timestamp, 
           log.status_id, 
           log.message, log.dm_load_id, log.load_id,
           log.start_date AS start_timestamp, 
           log.server_id,
           log.retry_attempts,
           job.job_type_id, 
           job.project_id, 
           job.name,
           job.description, 
           job."action" 
      FROM (job 
            LEFT JOIN job_log log 
            ON (((job.job_id)::text = (log.job_id)::text))) 
      ORDER BY 
           CASE log.status_id 
	        WHEN 1 THEN 'A: Running'::bpchar 
	        WHEN 2 THEN 'B: Error'::bpchar 
	        WHEN 3 THEN 'F: Finished'::bpchar 
	        WHEN 4 THEN 'C: Ready to run'::bpchar 
	        WHEN 12 THEN 'D:  Waiting to retry'::bpchar 
	        WHEN 5 THEN 'E: Waiting for children'::bpchar 
	        WHEN 6 THEN 'B: Error'::bpchar 
	        WHEN 7 THEN 'F: Finished'::bpchar 
	        ELSE 'G: Not in project'::bpchar END, 
	    COALESCE((log.end_date)::timestamp with time zone, ('now'::text)::timestamp(6) with time zone) DESC, 
            (COALESCE((log.end_date)::timestamp with time zone, ('now'::text)::timestamp(6) with time zone) - (log.execution_date)::timestamp with time zone) DESC,
            log.dm_load_id,
            job.job_id;


ALTER TABLE job_progress OWNER TO ketlmd;

--
-- TOC entry 1448 (class 1259 OID 205753162)
-- Dependencies: 9
-- Name: job_qa_hist; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_qa_hist (
    job_id character varying(60) NOT NULL,
    qa_id character varying(255) NOT NULL,
    qa_type character varying(28) NOT NULL,
    step_name character varying(255) NOT NULL,
    details character varying(4000) NOT NULL,
    record_date timestamp without time zone NOT NULL
);


ALTER TABLE job_qa_hist OWNER TO ketlmd;

--
-- TOC entry 1458 (class 1259 OID 205753192)
-- Dependencies: 9
-- Name: job_schedule; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_schedule (
    schedule_id integer NOT NULL,
    job_id character varying(60) NOT NULL,
    "month" integer,
    month_of_year integer,
    "day" integer,
    day_of_week integer,
    day_of_month integer,
    hour_of_day integer,
    "hour" integer,
    next_run_date timestamp without time zone,
    schedule_desc character varying(255),
    minute_of_hour integer,
    "minute" integer,
    start_run_date timestamp without time zone,
    stop_run_date timestamp without time zone
);


ALTER TABLE job_schedule OWNER TO ketlmd;

--
-- TOC entry 1449 (class 1259 OID 205753167)
-- Dependencies: 9
-- Name: job_status; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_status (
    status_id int2 NOT NULL,
    status_desc character varying(30)
);


ALTER TABLE job_status OWNER TO ketlmd;

--
-- TOC entry 1450 (class 1259 OID 205753169)
-- Dependencies: 9
-- Name: job_type; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE job_type (
    job_type_id integer NOT NULL,
    description character varying(20),
    class_name character varying(50)
);


ALTER TABLE job_type OWNER TO ketlmd;

--
-- TOC entry 1459 (class 1259 OID 205753194)
-- Dependencies: 9
-- Name: load; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE "load" (
    load_id integer NOT NULL,
    start_job_id character varying(60),
    start_date timestamp without time zone,
    project_id integer,
    end_date timestamp without time zone,
    ignored_parents character(1),
    failed character(1)
);


ALTER TABLE "load" OWNER TO ketlmd;

--
-- TOC entry 1466 (class 1259 OID 205753357)
-- Dependencies: 9
-- Name: load_id; Type: SEQUENCE; Schema: ketlmd; Owner: ketlmd
--

CREATE SEQUENCE load_id
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 0
    CACHE 1;


ALTER TABLE load_id OWNER TO ketlmd;

--
-- TOC entry 1451 (class 1259 OID 205753171)
-- Dependencies: 9
-- Name: mail_server_detail; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE mail_server_detail (
    mailserver_id integer NOT NULL,
    hostname character varying(256),
    login character varying(256),
    pwd character varying(256),
    from_address character varying(50)
);


ALTER TABLE mail_server_detail OWNER TO ketlmd;

--
-- TOC entry 1460 (class 1259 OID 205753196)
-- Dependencies: 9
-- Name: parameter; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE parameter (
    parameter_id integer NOT NULL,
    parameter_list_id integer,
    parameter_name character varying(50),
    parameter_value character varying(4000),
    sub_parameter_list_id integer,
    sub_parameter_list_name character varying(50)
);


ALTER TABLE parameter OWNER TO ketlmd;

--
-- TOC entry 1452 (class 1259 OID 205753176)
-- Dependencies: 9
-- Name: parameter_list; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE parameter_list (
    parameter_list_id integer NOT NULL,
    parameter_list_name character varying(255) NOT NULL
);


ALTER TABLE parameter_list OWNER TO ketlmd;

--
-- TOC entry 1453 (class 1259 OID 205753178)
-- Dependencies: 9
-- Name: project; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE project (
    project_id integer NOT NULL,
    project_desc character varying(255)
);


ALTER TABLE project OWNER TO ketlmd;

--
-- TOC entry 1461 (class 1259 OID 205753201)
-- Dependencies: 9
-- Name: server; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE server (
    server_id integer NOT NULL,
    server_name character varying(256) NOT NULL,
    status_id integer NOT NULL,
    shutdown_now character(1),
    start_time timestamp without time zone,
    shutdown_time timestamp without time zone,
    last_ping_time timestamp without time zone
);


ALTER TABLE server OWNER TO ketlmd;

--
-- TOC entry 1462 (class 1259 OID 205753203)
-- Dependencies: 9
-- Name: server_executor; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE server_executor (
    server_id integer NOT NULL,
    job_executor_id integer NOT NULL,
    threads integer,
    queue_size integer,
	pool text
);


ALTER TABLE server_executor OWNER TO ketlmd;

--
-- TOC entry 1467 (class 1259 OID 205753359)
-- Dependencies: 9
-- Name: server_id; Type: SEQUENCE; Schema: ketlmd; Owner: ketlmd
--

CREATE SEQUENCE server_id
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 0
    CACHE 1;


ALTER TABLE server_id OWNER TO ketlmd;

--
-- TOC entry 1454 (class 1259 OID 205753180)
-- Dependencies: 9
-- Name: server_status; Type: TABLE; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE TABLE server_status (
    status_id integer NOT NULL,
    status_desc character varying(50) NOT NULL
);


ALTER TABLE server_status OWNER TO ketlmd;


CREATE TABLE field_mapping(
 field_name character varying(128) not null,
 mapping_name character varying(50)  not null,
 config text
);


ALTER TABLE field_mapping OWNER TO ketlmd;

ALTER TABLE ONLY field_mapping
    ADD CONSTRAINT field_mapping_pkey PRIMARY KEY (mapping_name,field_name);

--
-- TOC entry 1806 (class 16386 OID 205753222)
-- Dependencies: 1443 1443
-- Name: alert_address_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY alert_address
    ADD CONSTRAINT alert_address_pkey PRIMARY KEY (address_id);


ALTER INDEX alert_address_pkey OWNER TO ketlmd;

--
-- TOC entry 1829 (class 16386 OID 205753244)
-- Dependencies: 1456 1456 1456
-- Name: job_dependencie_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_dependencie
    ADD CONSTRAINT job_dependencie_pkey PRIMARY KEY (job_id, parent_job_id);


ALTER INDEX job_dependencie_pkey OWNER TO ketlmd;

--
-- TOC entry 1834 (class 16386 OID 205753246)
-- Dependencies: 1457 1457 1457
-- Name: job_executor_job_type_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_executor_job_type
    ADD CONSTRAINT job_executor_job_type_pkey PRIMARY KEY (job_executor_id, job_type_id);


ALTER INDEX job_executor_job_type_pkey OWNER TO ketlmd;

--
-- TOC entry 1808 (class 16386 OID 205753224)
-- Dependencies: 1446 1446
-- Name: job_executor_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_executor
    ADD CONSTRAINT job_executor_pkey PRIMARY KEY (job_executor_id);


ALTER INDEX job_executor_pkey OWNER TO ketlmd;

--
-- TOC entry 1810 (class 16386 OID 205753228)
-- Dependencies: 1447 1447 1447
-- Name: job_log_hist_job_id_key; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_log_hist
    ADD CONSTRAINT job_log_hist_job_id_key UNIQUE (job_id, load_id);


ALTER INDEX job_log_hist_job_id_key OWNER TO ketlmd;

--
-- TOC entry 1812 (class 16386 OID 205753226)
-- Dependencies: 1447 1447
-- Name: job_log_hist_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_log_hist
    ADD CONSTRAINT job_log_hist_pkey PRIMARY KEY (dm_load_id);


ALTER INDEX job_log_hist_pkey OWNER TO ketlmd;

--
-- TOC entry 1846 (class 16386 OID 205753260)
-- Dependencies: 1464 1464 1464
-- Name: job_log_job_id_key; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_log
    ADD CONSTRAINT job_log_job_id_key UNIQUE (job_id, load_id);


ALTER INDEX job_log_job_id_key OWNER TO ketlmd;

--
-- TOC entry 1848 (class 16386 OID 205753258)
-- Dependencies: 1464 1464
-- Name: job_log_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_log
    ADD CONSTRAINT job_log_pkey PRIMARY KEY (dm_load_id);


ALTER INDEX job_log_pkey OWNER TO ketlmd;

--
-- TOC entry 1827 (class 16386 OID 205753242)
-- Dependencies: 1455 1455
-- Name: job_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_pkey PRIMARY KEY (job_id);


ALTER INDEX job_pkey OWNER TO ketlmd;

--
-- TOC entry 1836 (class 16386 OID 205753248)
-- Dependencies: 1458 1458
-- Name: job_schedule_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_schedule
    ADD CONSTRAINT job_schedule_pkey PRIMARY KEY (schedule_id);


ALTER INDEX job_schedule_pkey OWNER TO ketlmd;

--
-- TOC entry 1815 (class 16386 OID 205753230)
-- Dependencies: 1449 1449
-- Name: job_status_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_status
    ADD CONSTRAINT job_status_pkey PRIMARY KEY (status_id);


ALTER INDEX job_status_pkey OWNER TO ketlmd;

--
-- TOC entry 1817 (class 16386 OID 205753232)
-- Dependencies: 1450 1450
-- Name: job_type_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY job_type
    ADD CONSTRAINT job_type_pkey PRIMARY KEY (job_type_id);


ALTER INDEX job_type_pkey OWNER TO ketlmd;

--
-- TOC entry 1838 (class 16386 OID 205753250)
-- Dependencies: 1459 1459
-- Name: load_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY "load"
    ADD CONSTRAINT load_pkey PRIMARY KEY (load_id);


ALTER INDEX load_pkey OWNER TO ketlmd;

--
-- TOC entry 1819 (class 16386 OID 205753234)
-- Dependencies: 1451 1451
-- Name: mail_server_detail_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY mail_server_detail
    ADD CONSTRAINT mail_server_detail_pkey PRIMARY KEY (mailserver_id);


ALTER INDEX mail_server_detail_pkey OWNER TO ketlmd;

--
-- TOC entry 1821 (class 16386 OID 205753236)
-- Dependencies: 1452 1452
-- Name: parameter_list_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY parameter_list
    ADD CONSTRAINT parameter_list_pkey PRIMARY KEY (parameter_list_id);


ALTER INDEX parameter_list_pkey OWNER TO ketlmd;

--
-- TOC entry 1840 (class 16386 OID 205753252)
-- Dependencies: 1460 1460
-- Name: parameter_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY parameter
    ADD CONSTRAINT parameter_pkey PRIMARY KEY (parameter_id);


ALTER INDEX parameter_pkey OWNER TO ketlmd;

--
-- TOC entry 1823 (class 16386 OID 205753238)
-- Dependencies: 1453 1453
-- Name: project_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY project
    ADD CONSTRAINT project_pkey PRIMARY KEY (project_id);


ALTER INDEX project_pkey OWNER TO ketlmd;

--
-- TOC entry 1844 (class 16386 OID 205753256)
-- Dependencies: 1462 1462 1462
-- Name: server_executor_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY server_executor
    ADD CONSTRAINT server_executor_pkey PRIMARY KEY (server_id, job_executor_id);


ALTER INDEX server_executor_pkey OWNER TO ketlmd;

--
-- TOC entry 1842 (class 16386 OID 205753254)
-- Dependencies: 1461 1461
-- Name: server_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY server
    ADD CONSTRAINT server_pkey PRIMARY KEY (server_id);


ALTER INDEX server_pkey OWNER TO ketlmd;

--
-- TOC entry 1825 (class 16386 OID 205753240)
-- Dependencies: 1454 1454
-- Name: server_status_pkey; Type: CONSTRAINT; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

ALTER TABLE ONLY server_status
    ADD CONSTRAINT server_status_pkey PRIMARY KEY (status_id);


ALTER INDEX server_status_pkey OWNER TO ketlmd;

--
-- TOC entry 1830 (class 1259 OID 205753205)
-- Dependencies: 1456
-- Name: job_id_dep_idx; Type: INDEX; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE INDEX job_id_dep_idx ON job_dependencie USING btree (job_id);


ALTER INDEX job_id_dep_idx OWNER TO ketlmd;

--
-- TOC entry 1831 (class 1259 OID 205753206)
-- Dependencies: 1456 1456
-- Name: op_dep_idx; Type: INDEX; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE UNIQUE INDEX op_dep_idx ON job_dependencie USING btree (parent_job_id, job_id);


ALTER INDEX op_dep_idx OWNER TO ketlmd;

--
-- TOC entry 1832 (class 1259 OID 205753207)
-- Dependencies: 1456
-- Name: par_job_id_dep_idx; Type: INDEX; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE INDEX par_job_id_dep_idx ON job_dependencie USING btree (parent_job_id);


ALTER INDEX par_job_id_dep_idx OWNER TO ketlmd;

--
-- TOC entry 1813 (class 1259 OID 205753182)
-- Dependencies: 1448 1448 1448 1448 1448
-- Name: pk_job_qa_history; Type: INDEX; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE UNIQUE INDEX pk_job_qa_history ON job_qa_hist USING btree (job_id, qa_id, qa_type, step_name, record_date);


ALTER INDEX pk_job_qa_history OWNER TO ketlmd;

--
-- TOC entry 1849 (class 1259 OID 205753215)
-- Dependencies: 1464
-- Name: xie1job_log; Type: INDEX; Schema: ketlmd; Owner: ketlmd; Tablespace: 
--

CREATE INDEX xie1job_log ON job_log USING btree (status_id);


ALTER INDEX xie1job_log OWNER TO ketlmd;

--
-- TOC entry 1866 (class 16386 OID 205753329)
-- Dependencies: 1443 1805 1463
-- Name: alert_subscription_address_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY alert_subscription
    ADD CONSTRAINT alert_subscription_address_id_fkey FOREIGN KEY (address_id) REFERENCES alert_address(address_id);


--
-- TOC entry 1867 (class 16386 OID 205753325)
-- Dependencies: 1455 1463 1826
-- Name: alert_subscription_job_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY alert_subscription
    ADD CONSTRAINT alert_subscription_job_id_fkey FOREIGN KEY (job_id) REFERENCES job(job_id);


--
-- TOC entry 1868 (class 16386 OID 205753321)
-- Dependencies: 1453 1463 1822
-- Name: alert_subscription_project_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY alert_subscription
    ADD CONSTRAINT alert_subscription_project_id_fkey FOREIGN KEY (project_id) REFERENCES project(project_id);


--
-- TOC entry 1855 (class 16386 OID 205753285)
-- Dependencies: 1826 1456 1455
-- Name: job_dependencie_job_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_dependencie
    ADD CONSTRAINT job_dependencie_job_id_fkey FOREIGN KEY (job_id) REFERENCES job(job_id);


--
-- TOC entry 1856 (class 16386 OID 205753281)
-- Dependencies: 1456 1826 1455
-- Name: job_dependencie_parent_job_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_dependencie
    ADD CONSTRAINT job_dependencie_parent_job_id_fkey FOREIGN KEY (parent_job_id) REFERENCES job(job_id);


--
-- TOC entry 1872 (class 16386 OID 205753349)
-- Dependencies: 1465 1464 1847
-- Name: job_error_dm_load_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_error
    ADD CONSTRAINT job_error_dm_load_id_fkey FOREIGN KEY (dm_load_id) REFERENCES job_log(dm_load_id);


--
-- TOC entry 1873 (class 16386 OID 205753345)
-- Dependencies: 1455 1826 1465
-- Name: job_error_job_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_error
    ADD CONSTRAINT job_error_job_id_fkey FOREIGN KEY (job_id) REFERENCES job(job_id);


--
-- TOC entry 1857 (class 16386 OID 205753293)
-- Dependencies: 1446 1807 1457
-- Name: job_executor_job_type_job_executor_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_executor_job_type
    ADD CONSTRAINT job_executor_job_type_job_executor_id_fkey FOREIGN KEY (job_executor_id) REFERENCES job_executor(job_executor_id);


--
-- TOC entry 1858 (class 16386 OID 205753289)
-- Dependencies: 1816 1450 1457
-- Name: job_executor_job_type_job_type_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_executor_job_type
    ADD CONSTRAINT job_executor_job_type_job_type_id_fkey FOREIGN KEY (job_type_id) REFERENCES job_type(job_type_id);


--
-- TOC entry 1852 (class 16386 OID 205753269)
-- Dependencies: 1450 1455 1816
-- Name: job_job_type_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_job_type_id_fkey FOREIGN KEY (job_type_id) REFERENCES job_type(job_type_id);


--
-- TOC entry 1850 (class 16386 OID 205753277)
-- Dependencies: 1450 1455 1816
-- Name: job_job_type_id_fkey1; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_job_type_id_fkey1 FOREIGN KEY (job_type_id) REFERENCES job_type(job_type_id);


--
-- TOC entry 1869 (class 16386 OID 205753341)
-- Dependencies: 1464 1455 1826
-- Name: job_log_job_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_log
    ADD CONSTRAINT job_log_job_id_fkey FOREIGN KEY (job_id) REFERENCES job(job_id);


--
-- TOC entry 1870 (class 16386 OID 205753337)
-- Dependencies: 1837 1459 1464
-- Name: job_log_load_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_log
    ADD CONSTRAINT job_log_load_id_fkey FOREIGN KEY (load_id) REFERENCES "load"(load_id);


--
-- TOC entry 1871 (class 16386 OID 205753333)
-- Dependencies: 1449 1814 1464
-- Name: job_log_status_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_log
    ADD CONSTRAINT job_log_status_id_fkey FOREIGN KEY (status_id) REFERENCES job_status(status_id);


--
-- TOC entry 1854 (class 16386 OID 205753261)
-- Dependencies: 1455 1452 1820
-- Name: job_parameter_list_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_parameter_list_id_fkey FOREIGN KEY (parameter_list_id) REFERENCES parameter_list(parameter_list_id);


--
-- TOC entry 1853 (class 16386 OID 205753265)
-- Dependencies: 1455 1822 1453
-- Name: job_project_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_project_id_fkey FOREIGN KEY (project_id) REFERENCES project(project_id);


--
-- TOC entry 1851 (class 16386 OID 205753273)
-- Dependencies: 1822 1453 1455
-- Name: job_project_id_fkey1; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_project_id_fkey1 FOREIGN KEY (project_id) REFERENCES project(project_id);


--
-- TOC entry 1859 (class 16386 OID 205753297)
-- Dependencies: 1455 1826 1458
-- Name: job_schedule_job_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY job_schedule
    ADD CONSTRAINT job_schedule_job_id_fkey FOREIGN KEY (job_id) REFERENCES job(job_id);


--
-- TOC entry 1860 (class 16386 OID 205753301)
-- Dependencies: 1820 1452 1460
-- Name: parameter_parameter_list_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY parameter
    ADD CONSTRAINT parameter_parameter_list_id_fkey FOREIGN KEY (parameter_list_id) REFERENCES parameter_list(parameter_list_id);


--
-- TOC entry 1865 (class 16386 OID 205753309)
-- Dependencies: 1462 1807 1446
-- Name: server_executor_job_executor_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY server_executor
    ADD CONSTRAINT server_executor_job_executor_id_fkey FOREIGN KEY (job_executor_id) REFERENCES job_executor(job_executor_id);


--
-- TOC entry 1862 (class 16386 OID 205753353)
-- Dependencies: 1446 1807 1462
-- Name: server_executor_job_executor_id_fkey1; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY server_executor
    ADD CONSTRAINT server_executor_job_executor_id_fkey1 FOREIGN KEY (job_executor_id) REFERENCES job_executor(job_executor_id);


--
-- TOC entry 1864 (class 16386 OID 205753313)
-- Dependencies: 1461 1841 1462
-- Name: server_executor_server_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY server_executor
    ADD CONSTRAINT server_executor_server_id_fkey FOREIGN KEY (server_id) REFERENCES server(server_id);


--
-- TOC entry 1863 (class 16386 OID 205753317)
-- Dependencies: 1461 1841 1462
-- Name: server_executor_server_id_fkey1; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY server_executor
    ADD CONSTRAINT server_executor_server_id_fkey1 FOREIGN KEY (server_id) REFERENCES server(server_id);


--
-- TOC entry 1861 (class 16386 OID 205753305)
-- Dependencies: 1461 1454 1824
-- Name: server_status_id_fkey; Type: FK CONSTRAINT; Schema: ketlmd; Owner: ketlmd
--

ALTER TABLE ONLY server
    ADD CONSTRAINT server_status_id_fkey FOREIGN KEY (status_id) REFERENCES server_status(status_id);


--
-- TOC entry 1880 (class 0 OID 0)
-- Name: DUMP TIMESTAMP; Type: DUMP TIMESTAMP; Schema: -; Owner: 
--

-- Completed on 2005-07-05 14:01:12 Pacific Standard Time


--
-- TOC entry 1877 (class 0 OID 0)
-- Dependencies: 9
-- Name: ketlmd; Type: ACL; Schema: -; Owner: ketlmd
--

GRANT ALL ON SCHEMA ketlmd TO ketlmd;



/* seed tables */
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
'12', 'Waiting to be retried'); 
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


-- JasperReports Extension
CREATE TABLE report_meta(
	rmid integer,
	name varchar(128),
	description varchar(255),
	filename varchar(128),
	report_type varchar(10),
	params varchar(255),
	data_interval varchar(22)
);

ALTER TABLE report_meta OWNER TO ketlmd;


ALTER TABLE report_meta
  ADD CONSTRAINT "PK_REPORT_META" PRIMARY KEY(rmid);
  


INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (1, 'Daily Traffic By Hour', NULL, 'DailyTrafficActivity.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (2, 'Weekly Traffic By Day', NULL, 'WeeklyTrafficActivity.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (3, 'All Traffic By Week', NULL, 'CumulativeTrafficActivity.jrxml', 'PREDEF', NULL, 'Cumulative');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (4, 'Top Referrers For Day', NULL, 'DailyTopReferrers.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (5, 'Top Referrers For Week', NULL, 'WeeklyTopReferrers.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (6, 'Top Referrers For All Weeks', NULL, 'CumulativeTopReferrers.jrxml', 'PREDEF', NULL, 'Cumulative');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (7, 'Top Search Engine Referrers For Day', NULL, 'DailyTopSearchEngineReferrers.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (8, 'Top Search Engine Referrers For Week', NULL, 'WeeklyTopSearchEngineReferrers.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (9, 'Top Search Engine Referrers For All Weeks', NULL, 'CumulativeTopReferrers.jrxml', 'PREDEF', 'subreport=SearchEngineReferrersByWeek;extraTitleText=SearchEngine ;outputFile=Cumulative_9', 'Cumulative');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (10, 'Top Exit Pages For Day', NULL, 'DailyExitPages.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (11, 'Top Exit Pages For Week', NULL, 'WeeklyExitPages.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (12, 'Top Exit Pages For All Weeks', NULL, 'CumulativeExitPages.jrxml', 'PREDEF', NULL, 'Cumulative');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (13, 'Top Entry Pages For Day', NULL, 'DailyEntryPages.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (14, 'Top Entry Pages For Week', NULL, 'WeeklyEntryPages.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (15, 'Top Entry Pages For All Weeks', NULL, 'CumulativeEntryPages.jrxml', 'PREDEF', NULL, 'Cumulative');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (16, 'Activity By Site For Day', NULL, 'DailySiteActivity.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (17, 'Activity By Site For Week', NULL, 'WeeklySiteActivity.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (18, 'Activity By Site For All Weeks', NULL, 'CumulativeSiteActivity.jrxml', 'PREDEF', NULL, 'Cumulative');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (19, 'Access By Country For Day', NULL, 'DailyGeographicActivity.jrxml', 'PREDEF', NULL, 'Daily');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (20, 'Access By Country For Week', NULL, 'WeeklyGeographicActivity.jrxml', 'PREDEF', NULL, 'Weekly');
INSERT INTO report_meta (rmid, name, description, filename, report_type, params, data_interval) VALUES (21, 'Access By Country For All Weeks', NULL, 'CumulativeGeographicActivity.jrxml', 'PREDEF', NULL, 'Cumulative');

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
 
CREATE FUNCTION nvl(anyelement, anyelement) RETURNS anyelement AS '
    SELECT COALESCE($1, $2);
    ' LANGUAGE SQL;
