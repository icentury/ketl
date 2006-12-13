-- Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
-- PostgreSQL database dump
--

SET client_encoding = 'UNICODE';
SET check_function_bodies = false;
SET client_min_messages = warning;
SET default_tablespace = '';

SET default_with_oids = true;
--
-- TOC entry 1779 (class 0 OID 0)
-- Name: DUMP TIMESTAMP; Type: DUMP TIMESTAMP; Schema: -; Owner: 
--

-- Started on 2005-07-05 14:14:47 Pacific Standard Time


--
-- TOC entry 11 (class 16672 OID 17522)
-- Name: WebClickStream; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA WebClickStream;


--
-- TOC entry 1783 (class 0 OID 0)
-- Dependencies: 11
-- Name: WebClickStream; Type: ACL; Schema: -; Owner: postgres
--


GRANT ALL ON SCHEMA WebClickStream TO GROUP dwh;

--
-- TOC entry 1782 (class 0 OID 0)
-- Dependencies: 11
-- Name: SCHEMA WebClickStream; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA WebClickStream IS 'Standard Web Clickstream schema on PostGreSQL';


SET search_path = WebClickStream, pg_catalog;

--
-- TOC entry 1289 (class 1259 OID 17536)
-- Dependencies: 11
-- Name: browser_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE browser_lu (
    browser_id integer NOT NULL,
    browser_desc text NOT NULL,
    mozilla_type text,
    compatible_type text,
    browser_version text,
    os_version text,
    browser_name text,
    possible_spider smallint,
    load_id integer
);


--
-- TOC entry 1290 (class 1259 OID 17541)
-- Dependencies: 11
-- Name: date_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE date_lu (
    date_id integer NOT NULL,
    month_nbr smallint NOT NULL,
    week_year_id integer,
    qtr_nbr smallint NOT NULL,
    week_nbr smallint NOT NULL,
    year_nbr smallint NOT NULL,
    date_desc date,
    day_of_week_nbr smallint,
    week_desc character varying(12),
    load_id integer
);


--
-- TOC entry 1291 (class 1259 OID 17543)
-- Dependencies: 11
-- Name: day_clickstream_next; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE day_clickstream_next (
    page_id integer,
    page_1_id integer,
    page_2_id integer,
    page_3_id integer,
    page_4_id integer,
    cnt integer,
    date_id integer,
    SITE_ID int4
);



--
-- TOC entry 1292 (class 1259 OID 17545)
-- Dependencies: 11
-- Name: day_clickstream_prev; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE day_clickstream_prev (
    date_id integer,
    page_id integer,
    prev_page_1_id integer,
    prev_page_2_id integer,
    prev_page_3_id integer,
    prev_page_4_id integer,
    cnt integer,
    SITE_ID int4
);


--
-- TOC entry 1293 (class 1259 OID 17547)
-- Dependencies: 11
-- Name: day_hit_subset_agg; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE day_hit_subset_agg (
    SITE_ID int4 NOT NULL,
    date_id integer NOT NULL,
    file_id integer NOT NULL,
    hits bigint,
    users integer,
    visits integer
);


--
-- TOC entry 1294 (class 1259 OID 17553)
-- Dependencies: 11
-- Name: DIM_SITE; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE DIM_SITE (
    SITE_ID int4 NOT NULL,
    SITE_NAME character varying(100) NOT NULL,
    domain_name character varying(50),
    insert_dttm timestamp with time zone NOT NULL
);


SET default_with_oids = false;

--
-- TOC entry 1342 (class 1259 OID 341391089)
-- Dependencies: 11
-- Name: dy_br_vmg_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE dy_br_vmg_agg (
    date_id integer NOT NULL,
    browser_id integer NOT NULL,
    visit_dur_min_grp integer NOT NULL,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer,
    pg_hit_cnt_summ bigint,
    rpt_visit_cnt_summ integer
);


--
-- TOC entry 1343 (class 1259 OID 341391091)
-- Dependencies: 11
-- Name: dy_br_vmg_pvg_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE dy_br_vmg_pvg_agg (
    date_id integer NOT NULL,
    browser_id integer NOT NULL,
    visit_dur_min_grp integer NOT NULL,
    SITE_ID int4 NOT NULL,
    pg_vw_per_visit_grp integer NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_hit_cnt_summ bigint
);


SET default_with_oids = true;

--
-- TOC entry 1338 (class 1259 OID 299524442)
-- Dependencies: 11
-- Name: dy_fl_stat_part; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE dy_fl_stat_part (
    SITE_ID int4 NOT NULL,
    date_id integer NOT NULL,
    file_id integer NOT NULL,
    status_code_id smallint NOT NULL,
    hits bigint
);


SET default_with_oids = false;

--
-- TOC entry 1344 (class 1259 OID 341391093)
-- Dependencies: 11
-- Name: dy_st_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE dy_st_agg (
    date_id integer NOT NULL,
    state text,
    country text,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer,
    rpt_visit_cnt_summ integer,
    visit_dur_sec_summ bigint
);


SET default_with_oids = true;



CREATE TABLE wk_st_agg
(
  week_year_id int4 NOT NULL,
  state text,
  country text,
  site_id int2 NOT NULL,
  user_cnt_summ int4,
  visit_cnt_summ int4,
  pg_vw_cnt_summ int8,
  rpt_visit_cnt_summ int4,
  visit_dur_sec_summ int8
);

ALTER TABLE wk_st_agg OWNER TO etl;



--
-- TOC entry 1295 (class 1259 OID 17555)
-- Dependencies: 11
-- Name: file_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE file_lu (
    file_id integer NOT NULL,
    file_url_string text,
    file_desc character varying(80),
    file_type character varying(50)
);


--
-- TOC entry 1296 (class 1259 OID 17560)
-- Dependencies: 1424 11
-- Name: file_type_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW file_type_lu AS
    SELECT DISTINCT file_lu.file_type FROM file_lu ORDER BY file_lu.file_type;


--
-- TOC entry 1337 (class 1259 OID 299524435)
-- Dependencies: 11
-- Name: geography_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE geography_lu (
    country text NOT NULL,
    state text NOT NULL
);


--
-- TOC entry 1325 (class 1259 OID 17660)
-- Dependencies: 11
-- Name: tmp_hit_hourly_a; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_hit_hourly_a (
    temp_session_id bigint,
    activity_dt timestamp with time zone,
    get_request text,
    status smallint,
    bytes_sent integer,
    canonical_server_port integer,
    referrer_url text,
    cleansed smallint,
    page_string text,
    server_name text,
    time_taken_to_serv_request integer
);


--
-- TOC entry 1329 (class 1259 OID 17695)
-- Dependencies: 11
-- Name: tmp_session_hourly_a; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_session_hourly_a (
    temp_session_id bigint NOT NULL,
    persistant_identifier text,
    first_session_activity timestamp with time zone,
    last_session_activity timestamp with time zone,
    ip_address text,
    browser text,
    referrer text,
    repeat_visitor smallint,
    start_persistant_identifier text,
    hits integer,
    pageviews integer,
    source_file text,
    country text,
    state text
);


--
-- TOC entry 1345 (class 1259 OID 341391106)
-- Dependencies: 1437 11
-- Name: hr_sess_dy_part_a; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW hr_sess_dy_part_a AS
    SELECT th.temp_session_id, th.activity_dt, th.get_request, th.status, th.bytes_sent, th.cleansed, th.time_taken_to_serv_request, ts.persistant_identifier, ts.first_session_activity, ts.last_session_activity, ts.ip_address, ts.repeat_visitor, ts.hits, ts.pageviews, ts.country, ts.state FROM (tmp_hit_hourly_a th JOIN tmp_session_hourly_a ts ON ((th.temp_session_id = ts.temp_session_id)));


--
-- TOC entry 1326 (class 1259 OID 17665)
-- Dependencies: 11
-- Name: tmp_hit_hourly_b; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_hit_hourly_b (
    temp_session_id bigint,
    activity_dt timestamp with time zone,
    get_request text,
    status smallint,
    bytes_sent integer,
    canonical_server_port integer,
    referrer_url text,
    cleansed smallint,
    page_string text,
    server_name text,
    time_taken_to_serv_request integer
);


--
-- TOC entry 1330 (class 1259 OID 17700)
-- Dependencies: 11
-- Name: tmp_session_hourly_b; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_session_hourly_b (
    temp_session_id bigint NOT NULL,
    persistant_identifier text,
    first_session_activity timestamp with time zone,
    last_session_activity timestamp with time zone,
    ip_address text,
    browser text,
    referrer text,
    repeat_visitor smallint,
    start_persistant_identifier text,
    hits integer,
    pageviews integer,
    source_file text,
    country text,
    state text
);


--
-- TOC entry 1346 (class 1259 OID 342576274)
-- Dependencies: 1438 11
-- Name: hr_sess_dy_part_b; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW hr_sess_dy_part_b AS
    SELECT th.temp_session_id, th.activity_dt, th.get_request, th.status, th.bytes_sent, th.cleansed, th.time_taken_to_serv_request, ts.persistant_identifier, ts.first_session_activity, ts.last_session_activity, ts.ip_address, ts.repeat_visitor, ts.hits, ts.pageviews, ts.country, ts.state FROM (tmp_hit_hourly_b th JOIN tmp_session_hourly_b ts ON ((th.temp_session_id = ts.temp_session_id)));


--
-- TOC entry 1297 (class 1259 OID 17563)
-- Dependencies: 11
-- Name: html_status_code_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE html_status_code_lu (
    status_code_id smallint NOT NULL,
    status_code_desc character varying(55)
);


--
-- TOC entry 1298 (class 1259 OID 17565)
-- Dependencies: 11
-- Name: idgenerator; Type: SEQUENCE; Schema: WebClickStream; Owner: postgres
--

CREATE SEQUENCE idgenerator
    START WITH 0
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 0
    CACHE 100;


--
-- TOC entry 1299 (class 1259 OID 17567)
-- Dependencies: 11
-- Name: ip_address_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE ip_address_lu (
    ip_address character varying(80) NOT NULL,
    hostname text,
    load_id integer
);


--
-- TOC entry 1300 (class 1259 OID 17569)
-- Dependencies: 1425 11
-- Name: month_year_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW month_year_lu AS
    SELECT date_lu.month_nbr, date_lu.year_nbr, COALESCE(max(to_char((date_lu.date_desc)::timestamp with time zone, 'Month'::text)), 'NA'::text) AS month_desc FROM date_lu GROUP BY date_lu.year_nbr, date_lu.month_nbr;


--
-- TOC entry 1301 (class 1259 OID 17577)
-- Dependencies: 11
-- Name: page_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE page_lu (
    page_id integer NOT NULL,
    url_string text,
    page_desc text,
    content_server_code character varying(20),
    url_path text,
    url_path_1 text,
    url_path_2 text,
    url_path_3 text,
    url_path_4 text,
    url_page text,
    load_id integer
);


--
-- TOC entry 1302 (class 1259 OID 17582)
-- Dependencies: 1426 11
-- Name: page_paths_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW page_paths_lu AS
    SELECT page_lu.url_path AS path_id FROM page_lu GROUP BY page_lu.url_path;


--
-- TOC entry 1303 (class 1259 OID 17585)
-- Dependencies: 1427 11
-- Name: path_1_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW path_1_lu AS
    SELECT page_lu.url_path AS url_path_1 FROM page_lu GROUP BY page_lu.url_path;


--
-- TOC entry 1304 (class 1259 OID 17588)
-- Dependencies: 11
-- Name: pmt_tmp_hit_hourly; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE pmt_tmp_hit_hourly (
    date_id integer,
    pbtname text
);


--
-- TOC entry 1347 (class 1259 OID 344034665)
-- Dependencies: 1439 11
-- Name: pmt_hr_sess_dy_part; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW pmt_hr_sess_dy_part AS
    SELECT -1 AS date_id, ('HR_SESS_DY_PART_'::text || "substring"(pmt_tmp_hit_hourly.pbtname, length(pmt_tmp_hit_hourly.pbtname), 1)) AS pbtname FROM pmt_tmp_hit_hourly;


--
-- TOC entry 1305 (class 1259 OID 17593)
-- Dependencies: 11
-- Name: pmt_tmp_session_hourly; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE pmt_tmp_session_hourly (
    date_id integer,
    pbtname text
);


--
-- TOC entry 1306 (class 1259 OID 17598)
-- Dependencies: 11
-- Name: session_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE session_lu (
    session_id bigint NOT NULL,
    start_timestamp timestamp with time zone,
    end_timestamp timestamp with time zone,
    repeat_visitor smallint,
    session_cookie_id text,
    browser_id integer,
    cookie_ok_flag character(1),
    ip_address text,
    referrer_url_string text,
    referrer_desc text,
    SITE_ID int4,
    server_desc text,
    server_address text,
    persistant_cookie_id text,
    country text,
    state text
);


--
-- TOC entry 1307 (class 1259 OID 17603)
-- Dependencies: 1428 11
-- Name: referrer_lu; Type: VIEW; Schema: WebClickStream; Owner: postgres
--

CREATE VIEW referrer_lu AS
    SELECT DISTINCT session_lu.referrer_desc FROM session_lu ORDER BY session_lu.referrer_desc;


--
-- TOC entry 1308 (class 1259 OID 17606)
-- Dependencies: 1429 11
-- Name: referrer_url_lu; Type: VIEW; Schema: WebClickStream; Owner: postgres
--

CREATE VIEW referrer_url_lu AS
    SELECT DISTINCT session_lu.referrer_url_string, session_lu.referrer_desc FROM session_lu ORDER BY session_lu.referrer_url_string, session_lu.referrer_desc;


--
-- TOC entry 1309 (class 1259 OID 17609)
-- Dependencies: 11
-- Name: seq_browser_id; Type: SEQUENCE; Schema: WebClickStream; Owner: postgres
--

CREATE SEQUENCE seq_browser_id
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 0
    CACHE 100;


--
-- TOC entry 1348 (class 1259 OID 346012736)
-- Dependencies: 11
-- Name: seq_SITE_ID; Type: SEQUENCE; Schema: WebClickStream; Owner: postgres
--

CREATE SEQUENCE seq_SITE_ID
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 0
    CACHE 100;


--
-- TOC entry 1310 (class 1259 OID 17611)
-- Dependencies: 11
-- Name: seq_file_id; Type: SEQUENCE; Schema: WebClickStream; Owner: postgres
--

CREATE SEQUENCE seq_file_id
    INCREMENT BY 1
    NO MAXVALUE
    MINVALUE 0
    CACHE 100;


--
-- TOC entry 1312 (class 1259 OID 17615)
-- Dependencies: 11
-- Name: seq_page_id; Type: SEQUENCE; Schema: WebClickStream; Owner: postgres
--

CREATE SEQUENCE seq_page_id
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 100;


--
-- TOC entry 1313 (class 1259 OID 17617)
-- Dependencies: 11
-- Name: seq_session_id; Type: SEQUENCE; Schema: WebClickStream; Owner: postgres
--

CREATE SEQUENCE seq_session_id
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 100;


--
-- TOC entry 1314 (class 1259 OID 17619)
-- Dependencies: 1692 11
-- Name: web_site_activity_fa; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE web_site_activity_fa (
    page_sequence_id integer,
    page_id integer,
    load_time_sec integer,
    total_page_hits_cnt integer,
    exit_page_flag smallint,
    total_page_bytes bigint,
    view_time_sec integer,
    session_id bigint,
    status_code_id smallint,
    time_id integer,
    date_id integer,
    SITE_ID integer DEFAULT -1
);


--
-- TOC entry 1315 (class 1259 OID 17622)
-- Dependencies: 1430 11
-- Name: session_fa; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW session_fa AS
    SELECT a12.state, a12.country, a12.persistant_cookie_id, a12.server_address, a12.server_desc, a12.SITE_ID, a12.referrer_desc, a12.referrer_url_string, a12.ip_address, a12.cookie_ok_flag, a12.browser_id, a12.session_cookie_id, a12.repeat_visitor, a12.end_timestamp, a12.start_timestamp, a12.session_id, web.status_code_id, web.time_id, web.date_id, web.page_sequence_id FROM session_lu a12, web_site_activity_fa web WHERE ((a12.session_id = web.session_id) AND (web.exit_page_flag = (1)::smallint));


--
-- TOC entry 1340 (class 1259 OID 299524453)
-- Dependencies: 11
-- Name: stg_date_list; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_date_list (
    date_id integer NOT NULL,
    week_only smallint NOT NULL,
    week_year_id integer
);


--
-- TOC entry 1316 (class 1259 OID 17625)
-- Dependencies: 11
-- Name: stg_files_to_monitor; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_files_to_monitor (
    file_type character varying(50),
    file_match_string character varying(20)
);


INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio/Video file', '%.asf(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio/Video playlist', '%.asx(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Video Clip', '%.avi(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('QuickTime Movie', '%.mov(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('RPM Archive', '%.rpm(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('MP3 audio file (mp3)', '%.mp3(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('RealPlayer file', '%.ram(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('GZIP Archive', '%.gz(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('ZIP Archive', '%.zip(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Adobe Acrobat PDF', '%.pdf(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('RealAudio Clip', '%.ra(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('RealAudio Clip', '%.ra(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Wave Sound', '%.wav(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio file', '%.wma(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio/Video file', '%.wmv(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio/Video playlist', '%.wmx(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio/Video file', '%.wm(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('Windows Media Audio/Video file', '%.wm(?| |/|.)%');
INSERT INTO stg_files_to_monitor (file_type, file_match_string) VALUES ('MPEG Audio/Video file', '%.mpeg(?| |/|.)%');

analyze stg_files_to_monitor;

--
-- TOC entry 1317 (class 1259 OID 17627)
-- Dependencies: 11
-- Name: stg_open_session; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_open_session (
    session_id bigint,
    temp_session_id bigint NOT NULL,
    first_session_activity timestamp with time zone,
    last_session_activity timestamp with time zone,
    repeat_visitor integer,
    cookie_id text,
    cookie_flag integer,
    ip_address text,
    referrer_url text,
    referrer_site text,
    browser_id integer,
    server_desc text,
    server_address text,
    page_views integer,
    hits integer,
    persistant_identifier text,
    country text,
    state text,
    SITE_ID integer,
    session_extension_cnt integer
);


--
-- TOC entry 1318 (class 1259 OID 17632)
-- Dependencies: 11
-- Name: stg_opn_sess_lst_pg; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_opn_sess_lst_pg (
    page_sequence_id integer,
    page_id integer,
    load_time_sec integer,
    total_page_hits_cnt integer,
    exit_page_flag smallint,
    total_page_bytes integer,
    view_time_sec integer,
    session_id bigint NOT NULL,
    status_code_id smallint,
    time_id integer,
    date_id integer,
    SITE_ID integer,
    temp_session_id bigint,
    first_session_activity timestamp with time zone
);


--
-- TOC entry 1319 (class 1259 OID 17634)
-- Dependencies: 11
-- Name: stg_pers_id_list; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_pers_id_list (
    persistant_identifier text NOT NULL,
    first_session_dt timestamp with time zone,
    load_id integer
);


--
-- TOC entry 1339 (class 1259 OID 299524446)
-- Dependencies: 11
-- Name: stg_sess_agg; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_sess_agg (
    session_id bigint NOT NULL,
    first_page_id integer NOT NULL,
    last_page_id integer NOT NULL,
    ip_address text,
    browser_id integer NOT NULL,
    pg_vw_per_visit integer NOT NULL,
    pg_vw_per_visit_grp integer NOT NULL,
    visit_dur_min integer NOT NULL,
    visit_dur_min_grp integer NOT NULL,
    referrer_desc text,
    state text,
    SITE_ID int4 NOT NULL,
    persistant_cookie_id text,
    pg_byt_summ bigint,
    pg_hit_cnt_summ bigint,
    repeat_visitor integer,
    visit_dur_sec_summ integer,
    load_id integer NOT NULL,
    country text
);


--
-- TOC entry 1341 (class 1259 OID 299524471)
-- Dependencies: 11
-- Name: stg_wsaf_agg; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_wsaf_agg (
    date_id integer NOT NULL,
    week_year_id integer NOT NULL,
    hour_grp_nbr smallint NOT NULL,
    page_id integer NOT NULL,
    session_id bigint NOT NULL,
    ip_address text,
    browser_id integer NOT NULL,
    visit_dur_min integer NOT NULL,
    visit_dur_min_grp integer NOT NULL,
    referrer_desc text,
    state text,
    SITE_ID int4 NOT NULL,
    status_code_id smallint NOT NULL,
    page_sequence_id integer NOT NULL,
    exit_page_flag smallint,
    persistant_cookie_id text,
    total_page_bytes bigint,
    total_page_hits_cnt bigint,
    repeat_visitor integer,
    visit_dur_sec_summ integer,
    load_id integer NOT NULL,
    country text
);


SET default_with_oids = false;

--
-- TOC entry 1360 (class 1259 OID 372364625)
-- Dependencies: 11
-- Name: stg_wsaf_agg_1; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE stg_wsaf_agg_1 (
    date_id integer NOT NULL,
    week_year_id integer NOT NULL,
    hour_grp_nbr smallint NOT NULL,
    page_id integer NOT NULL,
    session_id bigint NOT NULL,
    SITE_ID int4 NOT NULL,
    status_code_id smallint NOT NULL,
    page_sequence_id integer NOT NULL,
    exit_page_flag smallint,
    total_page_bytes bigint,
    total_page_hits_cnt bigint,
    visit_dur_sec_summ integer,
    load_id integer NOT NULL
);


--
-- TOC entry 1362 (class 1259 OID 515750573)
-- Dependencies: 11
-- Name: temp_session_referrer_url; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE temp_session_referrer_url (
    temp_session_id bigint,
    referrer_url text,
    server_name text
);


SET default_with_oids = true;

--
-- TOC entry 1320 (class 1259 OID 17644)
-- Dependencies: 11
-- Name: time_of_day_lu; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE time_of_day_lu (
    time_id integer NOT NULL,
    hour_nbr smallint NOT NULL,
    second_nbr smallint NOT NULL,
    minute_nbr smallint NOT NULL,
    load_id integer
);


--
-- TOC entry 1321 (class 1259 OID 17646)
-- Dependencies: 11
-- Name: tmp_clickstream_next; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_clickstream_next (
    page_id integer,
    page_1_id integer,
    page_2_id integer,
    page_3_id integer,
    page_4_id integer,
    date_id integer,
    SITE_ID int4
);


--
-- TOC entry 1322 (class 1259 OID 17648)
-- Dependencies: 11
-- Name: tmp_clickstream_prev; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_clickstream_prev (
    page_id integer,
    prev_page_1_id integer,
    prev_page_2_id integer,
    prev_page_3_id integer,
    prev_page_4_id integer,
    date_id integer,
    SITE_ID int4
);


--
-- TOC entry 1323 (class 1259 OID 17650)
-- Dependencies: 11
-- Name: tmp_downloads; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_downloads (
    temp_session_id bigint,
    file_type character varying(20),
    file_url_string text,
    date_desc date
);


--
-- TOC entry 1324 (class 1259 OID 17655)
-- Dependencies: 11
-- Name: tmp_hit; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_hit (
    temp_session_id bigint,
    activity_dt timestamp with time zone,
    get_request text,
    status smallint,
    bytes_sent integer,
    canonical_server_port integer,
    referrer_url text,
    cleansed smallint,
    page_string text,
    server_name text,
    time_taken_to_serv_request integer
);


SET default_with_oids = false;

--
-- TOC entry 1361 (class 1259 OID 515730667)
-- Dependencies: 11
-- Name: tmp_hit_subset; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_hit_subset (
    temp_session_id bigint,
    file_type character varying(50),
    file_url_string text,
    date_desc timestamp with time zone,
    status_code_id smallint
);


SET default_with_oids = true;

--
-- TOC entry 1327 (class 1259 OID 17685)
-- Dependencies: 11
-- Name: tmp_new_session; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_new_session (
    session_id bigint,
    temp_session_id bigint,
    first_session_activity timestamp with time zone,
    last_session_activity timestamp with time zone,
    repeat_visitor integer,
    cookie_id text,
    cookie_flag integer,
    ip_address text,
    referrer_url text,
    referrer_site text,
    browser_id integer,
    server_desc character varying,
    server_address character varying,
    page_views integer,
    hits integer,
    persistant_identifier text,
    country text,
    state text,
    SITE_ID integer,
    session_extension_cnt integer
);


--
-- TOC entry 1328 (class 1259 OID 17690)
-- Dependencies: 11
-- Name: tmp_session; Type: TABLE; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE TABLE tmp_session (
    temp_session_id bigint,
    persistant_identifier text,
    first_session_activity timestamp with time zone,
    last_session_activity timestamp with time zone,
    ip_address text,
    browser text,
    referrer text,
    repeat_visitor smallint,
    start_persistant_identifier text,
    hits integer,
    pageviews integer,
    source_file text,
    country text,
    state text
);


--
-- TOC entry 1331 (class 1259 OID 17705)
-- Dependencies: 1431 11
-- Name: url_path_1_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW url_path_1_lu AS
    SELECT page_lu.url_path_1 FROM page_lu GROUP BY page_lu.url_path_1;


--
-- TOC entry 1332 (class 1259 OID 17708)
-- Dependencies: 1432 11
-- Name: url_path_2_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW url_path_2_lu AS
    SELECT page_lu.url_path_1, page_lu.url_path_2 FROM page_lu GROUP BY page_lu.url_path_1, page_lu.url_path_2;


--
-- TOC entry 1333 (class 1259 OID 17711)
-- Dependencies: 1433 11
-- Name: url_path_3_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW url_path_3_lu AS
    SELECT page_lu.url_path_1, page_lu.url_path_2, page_lu.url_path_3 FROM page_lu GROUP BY page_lu.url_path_1, page_lu.url_path_2, page_lu.url_path_3;


--
-- TOC entry 1334 (class 1259 OID 17714)
-- Dependencies: 1434 11
-- Name: url_path_4_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW url_path_4_lu AS
    SELECT page_lu.url_path_1, page_lu.url_path_2, page_lu.url_path_3, page_lu.url_path_4 FROM page_lu GROUP BY page_lu.url_path_1, page_lu.url_path_2, page_lu.url_path_3, page_lu.url_path_4;


--
-- TOC entry 1335 (class 1259 OID 17734)
-- Dependencies: 1435 11
-- Name: week_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW week_lu AS
    SELECT date_lu.week_year_id, max((date_lu.week_desc)::text) AS week_desc, max(date_lu.date_desc) AS short_week_desc FROM date_lu GROUP BY date_lu.week_year_id;


--
-- TOC entry 1336 (class 1259 OID 17737)
-- Dependencies: 1436 11
-- Name: week_of_year_lu; Type: VIEW; Schema: WebClickStream; Owner: enetworks
--

CREATE VIEW week_of_year_lu AS
    SELECT DISTINCT date_lu.week_nbr FROM date_lu ORDER BY date_lu.week_nbr;


SET default_with_oids = false;

--
-- TOC entry 1349 (class 1259 OID 350052719)
-- Dependencies: 11
-- Name: wk_br_hrg_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_br_hrg_agg (
    week_year_id integer NOT NULL,
    browser_id integer NOT NULL,
    hour_grp_nbr smallint NOT NULL,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer
);




CREATE TABLE hit_subset_fa
(
  session_id int8,
  site_id int4,
  status_code_id int2,
  date_id int4,
  file_id int4,
  bytes_sent int4
);

--
-- TOC entry 1350 (class 1259 OID 350054250)
-- Dependencies: 11
-- Name: wk_hrg_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_hrg_agg (
    week_year_id integer NOT NULL,
    hour_grp_nbr smallint NOT NULL,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer
);


--
-- TOC entry 1351 (class 1259 OID 350054252)
-- Dependencies: 11
-- Name: wk_hrg_st_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_hrg_st_agg (
    week_year_id integer NOT NULL,
    hour_grp_nbr smallint NOT NULL,
    state text,
    country text,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ bigint,
    rpt_visit_cnt_summ integer,
    visit_dur_sec_summ bigint
);


--
-- TOC entry 1352 (class 1259 OID 350054671)
-- Dependencies: 11
-- Name: wk_ip_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_ip_agg (
    week_year_id integer NOT NULL,
    ip_address text,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer
);


--
-- TOC entry 1353 (class 1259 OID 350055670)
-- Dependencies: 11
-- Name: wk_pg_br_stat_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_pg_br_stat_agg (
    week_year_id integer NOT NULL,
    page_id integer NOT NULL,
    browser_id integer NOT NULL,
    SITE_ID int4 NOT NULL,
    status_code_id smallint NOT NULL,
    pg_vw_cnt_summ integer
);


--
-- TOC entry 1354 (class 1259 OID 350055672)
-- Dependencies: 11
-- Name: wk_pg_expg_part; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_pg_expg_part (
    week_year_id integer NOT NULL,
    page_id integer NOT NULL,
    SITE_ID int4 NOT NULL,
    exit_page_flag smallint,
    pg_vw_cnt_summ integer,
    user_cnt_summ integer,
	visit_cnt_summ integer,
	rpt_visit_cnt_summ integer,
	visit_dur_sec_summ integer
);


--
-- TOC entry 1355 (class 1259 OID 350057175)
-- Dependencies: 11
-- Name: wk_pg_seq_part; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_pg_seq_part (
    week_year_id integer NOT NULL,
    page_id integer NOT NULL,
    SITE_ID int4 NOT NULL,
    page_sequence_id integer NOT NULL,
    pg_vw_cnt_summ integer,
    user_cnt_summ int4,
    visit_cnt_summ int4
) ;
ALTER TABLE webclickstream.wk_pg_seq_part OWNER TO etl;


--
-- TOC entry 1356 (class 1259 OID 350057177)
-- Dependencies: 11
-- Name: wk_pvg_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_pvg_agg (
    week_year_id integer NOT NULL,
    SITE_ID int4 NOT NULL,
    pg_vw_per_visit_grp integer NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer
);


--
-- TOC entry 1357 (class 1259 OID 350057179)
-- Dependencies: 11
-- Name: wk_ref_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_ref_agg (
    week_year_id integer NOT NULL,
    referrer_desc text,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer
);


--
-- TOC entry 1358 (class 1259 OID 350058037)
-- Dependencies: 11
-- Name: wk_vmg_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_vmg_agg (
    week_year_id integer NOT NULL,
    visit_dur_min_grp integer NOT NULL,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    visit_dur_sec_summ bigint
);


--
-- TOC entry 1359 (class 1259 OID 350058039)
-- Dependencies: 11
-- Name: wk_vmg_st_agg; Type: TABLE; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE TABLE wk_vmg_st_agg (
    week_year_id integer NOT NULL,
    visit_dur_min_grp integer NOT NULL,
    state text,
    country text,
    SITE_ID int4 NOT NULL,
    user_cnt_summ integer,
    visit_cnt_summ integer,
    pg_vw_cnt_summ integer,
    rpt_visit_cnt_summ integer,
    visit_dur_sec_summ bigint
);


--
-- TOC entry 1738 (class 16386 OID 360383159)
-- Dependencies: 1329 1329
-- Name: IDX_TMP_SESSION_HR_A; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY tmp_session_hourly_a
    ADD CONSTRAINT "IDX_TMP_SESSION_HR_A" PRIMARY KEY (temp_session_id);


--
-- TOC entry 1740 (class 16386 OID 86164891)
-- Dependencies: 1330 1330
-- Name: IDX_TMP_SESSION_HR_B; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY tmp_session_hourly_b
    ADD CONSTRAINT "IDX_TMP_SESSION_HR_B" PRIMARY KEY (temp_session_id);


--
-- TOC entry 1695 (class 16386 OID 17750)
-- Dependencies: 1289 1289
-- Name: browser_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY browser_lu
    ADD CONSTRAINT browser_lu_pkey PRIMARY KEY (browser_id);


--
-- TOC entry 1697 (class 16386 OID 17752)
-- Dependencies: 1290 1290
-- Name: date_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY date_lu
    ADD CONSTRAINT date_lu_pkey PRIMARY KEY (date_id);


--
-- TOC entry 1703 (class 16386 OID 17754)
-- Dependencies: 1293 1293 1293 1293
-- Name: day_hit_subset_agg_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY day_hit_subset_agg
    ADD CONSTRAINT day_hit_subset_agg_pkey PRIMARY KEY (SITE_ID, date_id, file_id);


--
-- TOC entry 1705 (class 16386 OID 350083143)
-- Dependencies: 1294 1294
-- Name: DIM_SITE_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY DIM_SITE
    ADD CONSTRAINT DIM_SITE_pkey PRIMARY KEY (SITE_ID);

CREATE UNIQUE INDEX dim_site_domain_idx1
  ON dim_site
  USING btree
  (domain_name);
  
CREATE UNIQUE INDEX dim_site_domain_idx2
  ON dim_site
  USING btree
  ((COALESCE(domain_name, 'NA'::text)));
    
--
-- TOC entry 1744 (class 16386 OID 299524445)
-- Dependencies: 1338 1338 1338 1338 1338
-- Name: dy_fl_stat_part_pk; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY dy_fl_stat_part
    ADD CONSTRAINT dy_fl_stat_part_pk PRIMARY KEY (SITE_ID, date_id, file_id, status_code_id);


--
-- TOC entry 1707 (class 16386 OID 17760)
-- Dependencies: 1297 1297
-- Name: html_status_code_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY html_status_code_lu
    ADD CONSTRAINT html_status_code_lu_pkey PRIMARY KEY (status_code_id);


--
-- TOC entry 1742 (class 16386 OID 299524441)
-- Dependencies: 1337 1337 1337
-- Name: idx_geography_lu; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY geography_lu
    ADD CONSTRAINT idx_geography_lu PRIMARY KEY (state, country);


--
-- TOC entry 1709 (class 16386 OID 17762)
-- Dependencies: 1299 1299
-- Name: ip_address_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY ip_address_lu
    ADD CONSTRAINT ip_address_lu_pkey PRIMARY KEY (ip_address);


--
-- TOC entry 1711 (class 16386 OID 17764)
-- Dependencies: 1301 1301
-- Name: page_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY page_lu
    ADD CONSTRAINT page_lu_pkey PRIMARY KEY (page_id);


--
-- TOC entry 1715 (class 16386 OID 17766)
-- Dependencies: 1306 1306
-- Name: session_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY session_lu
    ADD CONSTRAINT session_lu_pkey PRIMARY KEY (session_id);


--
-- TOC entry 1749 (class 16386 OID 299524456)
-- Dependencies: 1340 1340
-- Name: stg_date_list_pk; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY stg_date_list
    ADD CONSTRAINT stg_date_list_pk PRIMARY KEY (date_id);


--
-- TOC entry 1725 (class 16386 OID 17768)
-- Dependencies: 1318 1318
-- Name: stg_opn_sess_pg_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY stg_opn_sess_lst_pg
    ADD CONSTRAINT stg_opn_sess_pg_pkey PRIMARY KEY (session_id);


--
-- TOC entry 1723 (class 16386 OID 17770)
-- Dependencies: 1317 1317
-- Name: stg_opn_sess_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY stg_open_session
    ADD CONSTRAINT stg_opn_sess_pkey PRIMARY KEY (temp_session_id);


--
-- TOC entry 1728 (class 16386 OID 17772)
-- Dependencies: 1319 1319
-- Name: stg_pers_id_list_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY stg_pers_id_list
    ADD CONSTRAINT stg_pers_id_list_pkey PRIMARY KEY (persistant_identifier);


--
-- TOC entry 1746 (class 16386 OID 299524452)
-- Dependencies: 1339 1339
-- Name: stg_sess_agg_pk; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY stg_sess_agg
    ADD CONSTRAINT stg_sess_agg_pk PRIMARY KEY (session_id);


--
-- TOC entry 1733 (class 16386 OID 17774)
-- Dependencies: 1320 1320
-- Name: time_of_day_lu_pkey; Type: CONSTRAINT; Schema: WebClickStream; Owner: etl; Tablespace: 
--

ALTER TABLE ONLY time_of_day_lu
    ADD CONSTRAINT time_of_day_lu_pkey PRIMARY KEY (time_id);


--
-- TOC entry 1693 (class 1259 OID 17797)
-- Dependencies: 1289
-- Name: browser_desc_unk; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX browser_desc_unk ON browser_lu USING btree (browser_desc);


--
-- TOC entry 1701 (class 1259 OID 1831707571)
-- Dependencies: 1291
-- Name: click_nxt_date_id; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX click_nxt_date_id ON day_clickstream_next USING btree (date_id);


--
-- TOC entry 1698 (class 1259 OID 1831707576)
-- Dependencies: 1290
-- Name: dt_id_pk; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX dt_id_pk ON date_lu USING btree (date_id);


--
-- TOC entry 1699 (class 1259 OID 1831707575)
-- Dependencies: 1290
-- Name: dt_wk_yr_id; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX dt_wk_yr_id ON date_lu USING btree (week_year_id);


--
-- TOC entry 1751 (class 1259 OID 350083049)
-- Dependencies: 1342
-- Name: dy_br_vmg_idx_br; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX dy_br_vmg_idx_br ON dy_br_vmg_agg USING btree (browser_id);


--
-- TOC entry 1752 (class 1259 OID 350083050)
-- Dependencies: 1342
-- Name: dy_br_vmg_idx_dy; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX dy_br_vmg_idx_dy ON dy_br_vmg_agg USING btree (date_id);


--
-- TOC entry 1753 (class 1259 OID 350083051)
-- Dependencies: 1343
-- Name: dy_br_vmg_pvg_idx_br; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX dy_br_vmg_pvg_idx_br ON dy_br_vmg_pvg_agg USING btree (browser_id);


--
-- TOC entry 1754 (class 1259 OID 350083052)
-- Dependencies: 1343
-- Name: dy_br_vmg_pvg_idx_dy; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX dy_br_vmg_pvg_idx_dy ON dy_br_vmg_pvg_agg USING btree (date_id);


--
-- TOC entry 1755 (class 1259 OID 350083053)
-- Dependencies: 1344
-- Name: dy_st_idx_dy; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX dy_st_idx_dy ON dy_st_agg USING btree (date_id);


--
-- TOC entry 1756 (class 1259 OID 350083054)
-- Dependencies: 1344 1344
-- Name: dy_st_idx_st; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX dy_st_idx_st ON dy_st_agg USING btree (state, country);


--
-- TOC entry 1700 (class 1259 OID 17798)
-- Dependencies: 1290
-- Name: idx_date_desc; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX idx_date_desc ON date_lu USING btree (date_desc);


--
-- TOC entry 1736 (class 1259 OID 515730666)
-- Dependencies: 1328
-- Name: idx_hit_s; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX idx_hit_s ON tmp_session USING btree (temp_session_id);


--
-- TOC entry 1734 (class 1259 OID 17800)
-- Dependencies: 1327
-- Name: idx_tmp_new_session; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX idx_tmp_new_session ON tmp_new_session USING btree (temp_session_id);


--
-- TOC entry 1735 (class 1259 OID 17801)
-- Dependencies: 1327
-- Name: idx_tmp_new_session2; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX idx_tmp_new_session2 ON tmp_new_session USING btree (session_id);

--
-- TOC entry 1735 (class 1259 OID 17801)
-- Dependencies: 1327
-- Name: idx_tmp_new_session2; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX idx_ip_addres ON ip_address_lu USING btree (ip_address);



--
-- TOC entry 1729 (class 1259 OID 17802)
-- Dependencies: 1320
-- Name: indx_hour_number; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX indx_hour_number ON time_of_day_lu USING btree (hour_nbr);


--
-- TOC entry 1730 (class 1259 OID 17803)
-- Dependencies: 1320
-- Name: indx_minute_number; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX indx_minute_number ON time_of_day_lu USING btree (minute_nbr);


--
-- TOC entry 1731 (class 1259 OID 17804)
-- Dependencies: 1320
-- Name: indx_second_number; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX indx_second_number ON time_of_day_lu USING btree (second_nbr);


--
-- TOC entry 1726 (class 1259 OID 17805)
-- Dependencies: 1319
-- Name: pk_stg_pers_id_list; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX pk_stg_pers_id_list ON stg_pers_id_list USING btree (persistant_identifier);


--
-- TOC entry 1716 (class 1259 OID 17806)
-- Dependencies: 1306 1306
-- Name: session_lu_session_length; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX session_lu_session_length ON session_lu USING btree (((end_timestamp - start_timestamp)));


--
-- TOC entry 1747 (class 1259 OID 350083055)
-- Dependencies: 1340
-- Name: stg_date_list_idx_dt; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX stg_date_list_idx_dt ON stg_date_list USING btree (date_id);


--
-- TOC entry 1778 (class 1259 OID 372364627)
-- Dependencies: 1360
-- Name: stg_wsaf_agg_1_idx_sess; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX stg_wsaf_agg_1_idx_sess ON stg_wsaf_agg_1 USING btree (session_id);


--
-- TOC entry 1750 (class 1259 OID 350083056)
-- Dependencies: 1341
-- Name: stg_wsaf_agg_idx_sess; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX stg_wsaf_agg_idx_sess ON stg_wsaf_agg USING btree (session_id);


--
-- TOC entry 1712 (class 1259 OID 17808)
-- Dependencies: 1301
-- Name: unk_url_string_idx; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE UNIQUE INDEX unk_url_string_idx ON page_lu USING btree (url_string);


--
-- TOC entry 1713 (class 1259 OID 17809)
-- Dependencies: 1301
-- Name: url_path_idx; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX url_path_idx ON page_lu USING btree (url_path);


--
-- TOC entry 1717 (class 1259 OID 17810)
-- Dependencies: 1314
-- Name: web_site_act_dt_id; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX web_site_act_dt_id ON web_site_activity_fa USING btree (date_id);


--
-- TOC entry 1718 (class 1259 OID 17811)
-- Dependencies: 1314 1314
-- Name: web_site_act_exit_pg; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX web_site_act_exit_pg ON web_site_activity_fa USING btree (date_id) WHERE (exit_page_flag = (1)::smallint);


--
-- TOC entry 1719 (class 1259 OID 17813)
-- Dependencies: 1314
-- Name: web_site_act_page_id; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX web_site_act_page_id ON web_site_activity_fa USING btree (page_sequence_id);


--
-- TOC entry 1720 (class 1259 OID 17814)
-- Dependencies: 1314
-- Name: web_site_act_pg_id; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX web_site_act_pg_id ON web_site_activity_fa USING btree (page_id);


--
-- TOC entry 1721 (class 1259 OID 17815)
-- Dependencies: 1314
-- Name: web_site_act_sess_id; Type: INDEX; Schema: WebClickStream; Owner: etl; Tablespace: 
--

CREATE INDEX web_site_act_sess_id ON web_site_activity_fa USING btree (session_id);


--
-- TOC entry 1757 (class 1259 OID 350083057)
-- Dependencies: 1349
-- Name: wk_br_hrg_idx_br; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_br_hrg_idx_br ON wk_br_hrg_agg USING btree (browser_id);


--
-- TOC entry 1758 (class 1259 OID 350083058)
-- Dependencies: 1349
-- Name: wk_br_hrg_idx_hrg; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_br_hrg_idx_hrg ON wk_br_hrg_agg USING btree (hour_grp_nbr);

CREATE INDEX hit_subset_fa_dt_id ON hit_subset_fa USING btree (date_id);


CREATE INDEX hit_subset_fa_file_id ON hit_subset_fa USING btree (file_id);


CREATE INDEX hit_subset_fa_sess_id ON hit_subset_fa USING btree (session_id);

--
-- TOC entry 1759 (class 1259 OID 350083059)
-- Dependencies: 1349
-- Name: wk_br_hrg_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_br_hrg_idx_wk ON wk_br_hrg_agg USING btree (week_year_id);


--
-- TOC entry 1760 (class 1259 OID 350083060)
-- Dependencies: 1350
-- Name: wk_hrg_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_hrg_idx_wk ON wk_hrg_agg USING btree (week_year_id);


--
-- TOC entry 1761 (class 1259 OID 350083061)
-- Dependencies: 1351 1351
-- Name: wk_hrg_st_idx_st; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_hrg_st_idx_st ON wk_hrg_st_agg USING btree (state, country);


--
-- TOC entry 1762 (class 1259 OID 350083062)
-- Dependencies: 1351
-- Name: wk_hrg_st_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_hrg_st_idx_wk ON wk_hrg_st_agg USING btree (week_year_id);


--
-- TOC entry 1763 (class 1259 OID 350083063)
-- Dependencies: 1352
-- Name: wk_ip_idx_ip; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_ip_idx_ip ON wk_ip_agg USING btree (ip_address);


--
-- TOC entry 1764 (class 1259 OID 350083064)
-- Dependencies: 1352
-- Name: wk_ip_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_ip_idx_wk ON wk_ip_agg USING btree (week_year_id);


--
-- TOC entry 1765 (class 1259 OID 350083065)
-- Dependencies: 1353
-- Name: wk_pg_br_stat_idx_br; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_br_stat_idx_br ON wk_pg_br_stat_agg USING btree (browser_id);


--
-- TOC entry 1766 (class 1259 OID 350083066)
-- Dependencies: 1353
-- Name: wk_pg_br_stat_idx_pg; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_br_stat_idx_pg ON wk_pg_br_stat_agg USING btree (page_id);


--
-- TOC entry 1767 (class 1259 OID 350083067)
-- Dependencies: 1353
-- Name: wk_pg_br_stat_idx_stat; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_br_stat_idx_stat ON wk_pg_br_stat_agg USING btree (status_code_id);


--
-- TOC entry 1768 (class 1259 OID 350083068)
-- Dependencies: 1353
-- Name: wk_pg_br_stat_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_br_stat_idx_wk ON wk_pg_br_stat_agg USING btree (week_year_id);


--
-- TOC entry 1769 (class 1259 OID 350083070)
-- Dependencies: 1354
-- Name: wk_pg_expg_idx_pg; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_expg_idx_pg ON wk_pg_expg_part USING btree (page_id);


--
-- TOC entry 1770 (class 1259 OID 350083071)
-- Dependencies: 1354
-- Name: wk_pg_expg_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_expg_idx_wk ON wk_pg_expg_part USING btree (week_year_id);


--
-- TOC entry 1771 (class 1259 OID 350083072)
-- Dependencies: 1355
-- Name: wk_pg_seq_idx_pg; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_seq_idx_pg ON wk_pg_seq_part USING btree (page_id);


--
-- TOC entry 1772 (class 1259 OID 350083074)
-- Dependencies: 1355
-- Name: wk_pg_seq_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pg_seq_idx_wk ON wk_pg_seq_part USING btree (week_year_id);


--
-- TOC entry 1773 (class 1259 OID 350083075)
-- Dependencies: 1356
-- Name: wk_pvg_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_pvg_idx_wk ON wk_pvg_agg USING btree (week_year_id);


--
-- TOC entry 1774 (class 1259 OID 350083076)
-- Dependencies: 1357
-- Name: wk_ref_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_ref_idx_wk ON wk_ref_agg USING btree (week_year_id);


--
-- TOC entry 1775 (class 1259 OID 350083077)
-- Dependencies: 1358
-- Name: wk_vmg_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_vmg_idx_wk ON wk_vmg_agg USING btree (week_year_id);


--
-- TOC entry 1776 (class 1259 OID 350083078)
-- Dependencies: 1359 1359
-- Name: wk_vmg_st_idx_st; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_vmg_st_idx_st ON wk_vmg_st_agg USING btree (state, country);


--
-- TOC entry 1777 (class 1259 OID 350083079)
-- Dependencies: 1359
-- Name: wk_vmg_st_idx_wk; Type: INDEX; Schema: WebClickStream; Owner: enetworks; Tablespace: 
--

CREATE INDEX wk_vmg_st_idx_wk ON wk_vmg_st_agg USING btree (week_year_id);


--
-- TOC entry 1840 (class 0 OID 0)
-- Name: DUMP TIMESTAMP; Type: DUMP TIMESTAMP; Schema: -; Owner: 
--

-- Completed on 2005-07-05 14:14:53 Pacific Standard Time




--
-- TOC entry 40 (class 1255 OID 17523)
-- Dependencies: 11 375
-- Name: cleangetrequest(text); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION cleangetrequest(get_request text) RETURNS text
    AS $$
  BEGIN
	return replace(trim(substring(get_request from '[ ].*[ ]')),'//','/');
	-- return tmpString;
  END;
$$
    LANGUAGE plpgsql IMMUTABLE;


--
-- TOC entry 42 (class 1255 OID 17524)
-- Dependencies: 11 375
-- Name: get_date_desc(integer); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION get_date_desc(date_id integer) RETURNS date
    AS $$
  DECLARE
	v_start_date	date := '1999-01-01';   -- min date in date_lu
  BEGIN
	IF (date_id is null) or (date_id = -1) or (date_id = 9999)
	THEN
		RETURN null;
	ELSIF (date_id=0)
	THEN
		RETURN (DATE '1950-01-01');
	ELSE
		RETURN (v_start_date + (date_id - 1));
	END IF;
  END;
$$
    LANGUAGE plpgsql IMMUTABLE;


--
-- TOC entry 44 (class 1255 OID 17525)
-- Dependencies: 11 375
-- Name: get_date_id(timestamp with time zone); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION get_date_id(i_date timestamp with time zone) RETURNS integer
    AS $$
  DECLARE
	v_start_date date := '01-JAN-99'; -- min date in date_lu
	v_end_date date := '18-MAY-2010';  -- max date in date_lu

  BEGIN
	IF i_date is null
	THEN
		RETURN -1;
	ELSIF (date_trunc('day',i_date) < v_start_date)
	THEN
		RETURN 0;
	ELSIF (date_trunc('day',i_date) > v_end_date)
	THEN
		RETURN 9999;
	ELSE
		RETURN (cast(date_trunc('day',i_date) as date) - v_start_date) + 1;
	END IF;
  END;
$$
    LANGUAGE plpgsql IMMUTABLE;


--
-- TOC entry 46 (class 1255 OID 17526)
-- Dependencies: 11 375
-- Name: get_job_id(bigint); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION get_job_id(job_id bigint) RETURNS bigint
    AS $$
  DECLARE
	x int2 := 0;
  BEGIN
	return job_id;
  END;
$$
    LANGUAGE plpgsql IMMUTABLE;


--
-- TOC entry 48 (class 1255 OID 17527)
-- Dependencies: 11 375
-- Name: get_time_desc(integer); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION get_time_desc(time_id integer) RETURNS time without time zone
    AS $$
  DECLARE
	v_start_time time := '00:00:00';
  BEGIN
	IF (time_id is null) or (time_id = -1)
	THEN
		RETURN null;
	ELSE
		RETURN (v_start_time + (interval '1 second' * time_id));
	END IF;
  END;
$$
    LANGUAGE plpgsql IMMUTABLE;


--
-- TOC entry 39 (class 1255 OID 17528)
-- Dependencies: 11 375
-- Name: getvariablevalue(text, text); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION getvariablevalue(purltoparse text, pvariable text) RETURNS text
    AS $$

-- Example:	purltoparse = "http://www.blah.com/virtualdir/folder/file.jsp?width=1024&height=768"
--		pvariable='width'

DECLARE
   endpos     INTEGER;
   varLength    INTEGER;
   startpos   INTEGER;

BEGIN
   startpos := position(pvariable in purltoparse);		-- startpos=48 (example)
   RAISE NOTICE 'startpos = %',startpos;

   IF startpos = 0
	THEN							-- variable not found in URL
		RETURN (null);
	ELSE							-- startpos = beginning of variable

		varLength := LENGTH (pvariable);		-- varLength = 5 (example)
		RAISE NOTICE 'varLength = %',varLength;

		endpos := position ('&' in substring(purltoparse from startpos+varLength)) + startpos+varLength-1;
								-- endpos = 58
		IF endpos > startpos+varLength-1
			THEN					-- next variable found, return text after variable and before next variable
				RAISE NOTICE 'endpos (&) = %',endpos;
				RETURN (substring(purltoparse from startpos+varLength for endpos-(startpos+varLength)));
			ELSE					-- next variable not found.
				endpos := position (' ' in substring(purltoparse from startpos + varlength))+startpos+varLength-1;
				RAISE NOTICE 'endpos ( ) = %',endpos;

				IF endpos = startpos+varLength-1
					THEN
						RETURN (substring(purltoparse from startpos+varLength));
					ELSE
						RETURN (substring(purltoparse from startpos+varLength for endpos-(startpos+varLength)));
				END IF;
		END IF;
   END IF;

END;

$$
    LANGUAGE plpgsql IMMUTABLE;


--
-- TOC entry 49 (class 1255 OID 17529)
-- Dependencies: 11 375
-- Name: load_web_activity_partitioned(integer, integer, integer); Type: FUNCTION; Schema: WebClickStream; Owner: postgres
--

CREATE FUNCTION load_web_activity_partitioned(job_id integer, partition_id integer, max_partitions integer) RETURNS boolean
    AS $$
  DECLARE
	curr_hit			RECORD;
	curr_pg				web_site_activity_fa%ROWTYPE;
	prev_pg				web_site_activity_fa%ROWTYPE;

	prev_hit_temp_session_id	int8 := -1;	-- Set this to something that will never match
	random_bytes			int8 := 0;	-- Pre-load to set random_bytes to 0 in first pass
	random_hits			int4 := 0;	-- Pre-load to set random_hits to 0 in first pass

	session_page_count		int8;		-- This stores the number of pages in the session being extracted.
							-- It is also used for page_sequence_id.

	session_start_tmstp		timestamp;	-- This stores the first activity_dt in the session.
	session_end_tmstp		timestamp;	-- This stores the last activity_dt in the session.

	curr_pg_start_tmstp		timestamp;	-- This stores the first activity_dt for the current hit page.
	curr_pg_end_tmstp		timestamp;	-- This stores the last activity_dt for the current hit page.

	prev_pg_start_tmstp		timestamp;	-- This stores the first activity_dt for the previous hit page.
	prev_pg_end_tmstp		timestamp;	-- This stores the last activity_dt for the previous hit page.

	last_page			int2;		-- This is set to 1 whenever prev_pg is the last page in a session.  Otherwise, null.
	new_sess			bool;		-- This is set true when session changes from previous hit record
	new_sess_pre_pg			bool;		-- This is set to true when session changes, and to false after first cleansed page
							-- It is used to collect extraneous web server hits before the first page hit in a session

	rec_ct				int2;		-- This is a temporary variable used to identify problems when looking up page_id and session_id

	cleaned_get_request		text;		-- This is a temporary variable used to improve performance of Page_LU lookup

	tmp_opn_pg			RECORD;		-- This record is used to store the data retrieved from tmp_opn_sess_lst_pg
	session_extension_cnt		int4 := 0;	-- This stores whether the number of times the current session has been extended from previous loads.
	page_record_exists		bool := FALSE;	-- This is set to true if the page needs to be updated in instead of inserted into web_site_activity_fa.

	loop_ct				int4 := 0;	-- This is a temporary instrumentation variable.
	
	start_tmstp			timestamptz;
	end_tmstp			timestamptz;

	SQL1				interval :=interval '0 seconds';
	SQL2				interval :=interval '0 seconds';
	SQL3				interval :=interval '0 seconds';
	SQL4				interval :=interval '0 seconds';
	SQL5				interval :=interval '0 seconds';
	SQL6				interval :=interval '0 seconds';
	SQL7				interval :=interval '0 seconds';
	SQL8				interval :=interval '0 seconds';
	SQL9				interval :=interval '0 seconds';
	SQL10				interval :=interval '0 seconds';
	SQL11				interval :=interval '0 seconds';
	SQL12				interval :=interval '0 seconds';
	SQL13				interval :=interval '0 seconds';
	SQL14				interval :=interval '0 seconds';
	SQL15				interval :=interval '0 seconds';
	SQL16				interval :=interval '0 seconds';
	SQL17				interval :=interval '0 seconds';
	SQL18				interval :=interval '0 seconds';
	SQL19				interval :=interval '0 seconds';
	

	SQLc1				int8 := 0;
	SQLc2				int8 := 0;
	SQLc3				int8 := 0;
	SQLc4				int8 := 0;
	SQLc5				int8 := 0;
	SQLc6				int8 := 0;
	SQLc7				int8 := 0;
	SQLc8				int8 := 0;
	SQLc9				int8 := 0;
	SQLc10				int8 := 0;
	SQLc11				int8 := 0;
	SQLc12				int8 := 0;
	SQLc13				int8 := 0;
	SQLc14				int8 := 0;
	SQLc15				int8 := 0;
	SQLc16				int8 := 0;
	SQLc17				int8 := 0;
	SQLc18				int8 := 0;
	SQLc19				int8 := 0;

--	start_tmstp := timeofday()::timestamptz

	msg_str				text;		-- This is a temporary text variable used for messages.

-- WARNING:	The tmp_clickstream_next and tmp_clickstream_prev implementations in this stored procedure do not use array variables,
--		for the reason that the behavior of array variables when dealing with nulls is suspect.

	no_pg				int4 := -2;	-- This constant value is used to indicate the absence of a page for clickstreams.
	no_site				int4 := -1;	-- This constant value is used to indicate the absence of a site for clickstreams.

	next_pg_a0	  		int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_a1	  		int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_a2	  		int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_a3	  		int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_a4	  		int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_ind			int2 := 0;	-- This index is used as a pointer into the array.

	next_pg_site_a0			int4 := -1;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_site_a1			int4 := -1;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_site_a2			int4 := -1;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_site_a3			int4 := -1;	-- This is a temporary variable used to pre-calculate clickstreams.
	next_pg_site_a4			int4 := -1;	-- This is a temporary variable used to pre-calculate clickstreams.

	prev_pg_a0			int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	prev_pg_a1			int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	prev_pg_a2			int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	prev_pg_a3			int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.
	prev_pg_a4			int4 := -2;	-- This is a temporary variable used to pre-calculate clickstreams.

        last_SITE_ID                      int2 := NULL;
        last_server_name                text := NULL;

  BEGIN

  -- Initialize session and page variables

	prev_pg.session_id := -1;			-- Force an invalid session for the first pass
	prev_pg.page_id := -1;				-- Force an invalid page for the first pass
	prev_pg.total_page_bytes := 0;			-- Pre-load to set random_bytes to 0 in first pass
	prev_pg.total_page_hits_cnt := 0;		-- Pre-load to set random_hits to 0 in first pass

  -- Loop

	FOR curr_hit in (select	th.temp_session_id, th.activity_dt, th.get_request, 
				th.status, coalesce(th.bytes_sent,0) as bytes_sent, th.cleansed,
				coalesce(th.time_taken_to_serv_request,0) / 1000000 as time_taken_to_serv_request, server_name
			 from	tmp_hit th 
			 where	(cast(th.temp_session_id as int8) % max_partitions) = partition_id
			 order by th.temp_session_id, th.activity_dt
			)
	LOOP

--		IF loop_ct % 100000 = 0
--			THEN
--				msg_str := trim(TO_CHAR(loop_ct,'99999999')) || ' at ' || timeofday() || ' for partition ' || TO_CHAR(partition_id,'9');
--				RAISE NOTICE '   Loops complete:  %', msg_str;
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 1, SQLc1, SQL1, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 2, SQLc2, SQL2, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 3, SQLc3, SQL3, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 4, SQLc4, SQL4, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 5, SQLc5, SQL5, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 6, SQLc6, SQL6, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 7, SQLc7, SQL7, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 8, SQLc8, SQL8, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 9, SQLc9, SQL9, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 10, SQLc10, SQL10, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 11, SQLc11, SQL11, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 12, SQLc12, SQL12, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 13, SQLc13, SQL13, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 14, SQLc14, SQL14, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 15, SQLc15, SQL15, partition_id, job_id);
--
--		END IF;
		loop_ct := loop_ct + 1;

--		msg_str :=	'Session='||to_char(curr_hit.temp_session_id,'999999999') || 
--				', Cleansed='||coalesce(to_char(curr_hit.cleansed,'9'),'0') ||
--				', Bytes='||coalesce(to_char(curr_hit.bytes_sent,'999999999'),'null') ||
--				', Time='||to_char(curr_hit.activity_dt,'yyyy-mm-dd hh:mm:ss');
--		RAISE LOG '  Next Hit: %', msg_str;

		new_sess := (curr_hit.temp_session_id <> prev_hit_temp_session_id);
		new_sess_pre_pg := (new_sess_pre_pg or new_sess);

		IF new_sess or ((curr_hit.cleansed=1) and not (new_sess_pre_pg))	-- Check for new session or page
			THEN								-- Write previous page, initialize new page
				IF ((prev_pg.page_id = -1) or (prev_pg.session_id = -1))
					THEN						-- Invalid previous page or session
						random_hits := random_hits + prev_pg.total_page_hits_cnt;
						random_bytes := random_bytes + prev_pg.total_page_bytes;

--						msg_str :=	'random_hits='||to_char(random_hits,'9999999') ||
--								', random_bytes='||to_char(random_bytes,'999999999');
--						RAISE LOG 'Random Data: %',msg_str;

					ELSE 						-- Valid previous page and session (write it)
						IF (prev_pg.load_time_sec < ceil(coalesce(extract('epoch' from (prev_pg_end_tmstp - prev_pg_start_tmstp)),0)))
							THEN
								prev_pg.load_time_sec := ceil(extract('epoch' from (prev_pg_end_tmstp - prev_pg_start_tmstp)));
						END IF;

						IF new_sess
							THEN				-- Use end-of-session logic for view-time
								last_page := 1;
								IF (session_page_count < 2)
									THEN
										prev_pg.view_time_sec := 7;
									ELSE
										prev_pg.view_time_sec := ceil(extract('epoch' from (session_end_tmstp - session_start_tmstp)) / (session_page_count - 1));
								END IF;
							ELSE				-- Use current-previous logic for view-time
								last_page := null;
								prev_pg.view_time_sec := ceil(extract('epoch' from (curr_hit.activity_dt - prev_pg_start_tmstp)));
						END IF;

						IF page_record_exists
							THEN
--								start_tmstp := timeofday()::timestamptz;

								UPDATE	web_site_activity_fa
								SET	load_time_sec=prev_pg.load_time_sec,
									total_page_hits_cnt=prev_pg.total_page_hits_cnt,
									view_time_sec=prev_pg.view_time_sec,
									total_page_bytes=prev_pg.total_page_bytes,
									exit_page_flag=last_page
								WHERE (session_id = prev_pg.session_id)
								  AND (page_sequence_id = prev_pg.page_sequence_id);

--								end_tmstp := timeofday()::timestamptz;

--								SQL1 := SQL1 + (end_tmstp - start_tmstp);
--								SQLc1 := SQLc1 + 1::int8;

--								msg_str :=	'session='||to_char(prev_pg.session_id,'999999999') ||
--										', page='||to_char(prev_pg.page_sequence_id,'9999');
--								RAISE LOG ' Updated: %',msg_str;

							ELSE
--								start_tmstp := timeofday()::timestamptz;

								INSERT INTO web_site_activity_fa(
									page_sequence_id, page_id, time_id, date_id, 
									session_id, load_time_sec, total_page_hits_cnt, 
									view_time_sec, total_page_bytes, exit_page_flag, 
									status_code_id, SITE_ID)
								VALUES(
									prev_pg.page_sequence_id, prev_pg.page_id, prev_pg.time_id, prev_pg.date_id, 
									prev_pg.session_id, prev_pg.load_time_sec, prev_pg.total_page_hits_cnt, 
									prev_pg.view_time_sec, prev_pg.total_page_bytes, last_page, 
									prev_pg.status_code_id, prev_pg.SITE_ID);

--								end_tmstp := timeofday()::timestamptz;
--								SQL2 := SQL2 + (end_tmstp - start_tmstp);
--								SQLc2 := SQLc2 + 1::int8;

--								msg_str :=	'session='||to_char(prev_pg.session_id,'999999999') ||
--										', page='||to_char(prev_pg.page_sequence_id,'9999');
--								RAISE LOG ' Inserted: %',msg_str;
						END IF;

						IF next_pg_ind = 0
							THEN	next_pg_a0 := prev_pg.page_id;
								next_pg_site_a0 := prev_pg.SITE_ID;
						ELSIF next_pg_ind = 1
							THEN	next_pg_a1 := prev_pg.page_id;
								next_pg_site_a1 := prev_pg.SITE_ID;
						ELSIF next_pg_ind = 2
							THEN	next_pg_a2 := prev_pg.page_id;
								next_pg_site_a2 := prev_pg.SITE_ID;
						ELSIF next_pg_ind = 3
							THEN	next_pg_a3 := prev_pg.page_id;
								next_pg_site_a3 := prev_pg.SITE_ID;
						ELSIF next_pg_ind = 4
							THEN	next_pg_a4 := prev_pg.page_id;
								next_pg_site_a4 := prev_pg.SITE_ID;
						ELSIF next_pg_ind > 4
							THEN
								next_pg_ind := 4;

--								start_tmstp := timeofday()::timestamptz;

								INSERT INTO tmp_clickstream_next (page_id, page_1_id, page_2_id, page_3_id, page_4_id, date_id, SITE_ID)
								VALUES (next_pg_a0,next_pg_a1,next_pg_a2,next_pg_a3,next_pg_a4,prev_pg.date_id,next_pg_site_a0);

--								end_tmstp := timeofday()::timestamptz;

--								SQL3 := SQL3 + (end_tmstp - start_tmstp);
--								SQLc3 := SQLc3 + 1::int8;

--								msg_str :=	'p0='||coalesce(trim(to_char(next_pg_a0,'999999')),'null')||
--										', p1='||coalesce(trim(to_char(next_pg_a1,'999999')),'null')||
--										', p2='||coalesce(trim(to_char(next_pg_a2,'999999')),'null')||
--										', p3='||coalesce(trim(to_char(next_pg_a3,'999999')),'null')||
--										', p4='||coalesce(trim(to_char(next_pg_a4,'999999')),'null');
--								RAISE LOG '   Click_next: %',msg_str;

							  -- Shift to next page in clickstream

								next_pg_a0 := next_pg_a1;
								next_pg_a1 := next_pg_a2;
								next_pg_a2 := next_pg_a3;
								next_pg_a3 := next_pg_a4;
								next_pg_a4 := prev_pg.page_id;

								next_pg_site_a0 := next_pg_site_a1;
								next_pg_site_a1 := next_pg_site_a2;
								next_pg_site_a2 := next_pg_site_a3;
								next_pg_site_a3 := next_pg_site_a4;
								next_pg_site_a4 := prev_pg.SITE_ID;

						END IF;

						next_pg_ind := next_pg_ind + 1;

						prev_pg_a4 := prev_pg_a3;
						prev_pg_a3 := prev_pg_a2;
						prev_pg_a2 := prev_pg_a1;
						prev_pg_a1 := prev_pg_a0;
						prev_pg_a0 := prev_pg.page_id;

--						start_tmstp := timeofday()::timestamptz;

						INSERT INTO tmp_clickstream_prev (page_id, prev_page_1_id, prev_page_2_id, prev_page_3_id, prev_page_4_id, date_id, SITE_ID)
						VALUES (prev_pg_a0,prev_pg_a1,prev_pg_a2,prev_pg_a3,prev_pg_a4,prev_pg.date_id,prev_pg.SITE_ID);

--						end_tmstp := timeofday()::timestamptz;

--						SQL4 := SQL4 + (end_tmstp - start_tmstp);
--						SQLc4 := SQLc4 + 1::int8;

--						msg_str :=	'p0='||coalesce(trim(to_char(prev_pg_a0,'999999')),'null')||
--								', p1='||coalesce(trim(to_char(prev_pg_a1,'999999')),'null')||
--								', p2='||coalesce(trim(to_char(prev_pg_a2,'999999')),'null')||
--								', p3='||coalesce(trim(to_char(prev_pg_a3,'999999')),'null')||
--								', p4='||coalesce(trim(to_char(prev_pg_a4,'999999')),'null');
--						RAISE LOG '   Click_prev: %',msg_str;

				END IF;

				curr_pg.page_id := -1;
				curr_pg_start_tmstp := curr_hit.activity_dt;
				curr_pg.total_page_hits_cnt := 0;
				curr_pg.total_page_bytes := 0;
				curr_pg.time_id := TO_NUMBER(TO_CHAR(curr_hit.activity_dt,'SSSS'),'S99999');
				curr_pg.date_id := get_date_id(curr_hit.activity_dt);
				curr_pg.load_time_sec := curr_hit.time_taken_to_serv_request;
				curr_pg.page_sequence_id := 0;
				page_record_exists := FALSE;

				IF new_sess
					THEN						-- This is a new session, so finish the previous and initialize
					  -- Finish writing next_page clickstream entries for this session

						FOR i IN 1..next_pg_ind LOOP
						  -- Write clickstream to database

--							start_tmstp := timeofday()::timestamptz;

							INSERT INTO tmp_clickstream_next (page_id, page_1_id, page_2_id, page_3_id, page_4_id, date_id, SITE_ID)
							VALUES (next_pg_a0,next_pg_a1,next_pg_a2,next_pg_a3,next_pg_a4,prev_pg.date_id,next_pg_site_a0);

--							end_tmstp := timeofday()::timestamptz;

--							SQL5 := SQL5 + (end_tmstp - start_tmstp);
--							SQLc5 := SQLc5 + 1::int8;

--							msg_str :=	'p0='||coalesce(trim(to_char(next_pg_a0,'999999')),'null')||
--									', p1='||coalesce(trim(to_char(next_pg_a1,'999999')),'null')||
--									', p2='||coalesce(trim(to_char(next_pg_a2,'999999')),'null')||
--									', p3='||coalesce(trim(to_char(next_pg_a3,'999999')),'null')||
--									', p4='||coalesce(trim(to_char(next_pg_a4,'999999')),'null');
--							RAISE LOG '   Click_next: %',msg_str;

						  -- Shift to next page in clickstream

							next_pg_a0 := next_pg_a1;
							next_pg_a1 := next_pg_a2;
							next_pg_a2 := next_pg_a3;
							next_pg_a3 := next_pg_a4;
							next_pg_a4 := no_pg;

							next_pg_site_a0 := next_pg_site_a1;
							next_pg_site_a1 := next_pg_site_a2;
							next_pg_site_a2 := next_pg_site_a3;
							next_pg_site_a3 := next_pg_site_a4;
							next_pg_site_a4 := no_site;

						END LOOP;

						next_pg_ind := 0;

						prev_pg_a0 := no_pg;
						prev_pg_a1 := no_pg;
						prev_pg_a2 := no_pg;
						prev_pg_a3 := no_pg;
						prev_pg_a4 := no_pg;

					  -- Initialize new session
						BEGIN

--							start_tmstp := timeofday()::timestamptz;

							select	max(tmp.session_id) as session_id,
								max(tmp.SITE_ID) as SITE_ID,
								max(tmp.session_extension_cnt) as sess_ext_cnt,
								count(*) as rec_cnt
							from tmp_new_session tmp 
							where tmp.temp_session_id = curr_hit.temp_session_id
							having count(*)=1
							into curr_pg.session_id, curr_pg.SITE_ID, session_extension_cnt,rec_ct;

--							end_tmstp := timeofday()::timestamptz;

--							SQL6 := SQL6 + (end_tmstp - start_tmstp);
--							SQLc6 := SQLc6 + 1::int8;

--							EXCEPTION
--								WHEN OTHERS THEN
--									curr_pg.session_id := -1;
--									curr_pg.SITE_ID := -1;
--									session_extension_cnt := 0;
						END;

						IF (curr_pg.session_id is null) or (rec_ct <> 1)
							THEN
								curr_pg.session_id := -1;
						END IF;

						IF (curr_pg.SITE_ID is null) or (rec_ct <> 1)
							THEN
								curr_pg.SITE_ID := -1;
						END IF;

						IF (session_extension_cnt > 0) and (rec_ct=1)
							THEN

--								msg_str := to_char(curr_pg.session_id,'999999999');
--								RAISE NOTICE 'Extended session: %', msg_str;

--								start_tmstp := timeofday()::timestamptz;

								select	coalesce(max(page_sequence_id),0) as page_sequence_id,
									coalesce(max(page_id),-1) as page_id,
									coalesce(max(load_time_sec),0) as load_time_sec,
									coalesce(max(total_page_hits_cnt),0) as total_page_hits,
									coalesce(max(total_page_bytes),0) as total_page_bytes,
									coalesce(max(view_time_sec),0) as view_time_sec,
									coalesce(max(status_code_id),-1) as status_code_id,
									coalesce(max(time_id),-1) as time_id,
									coalesce(max(date_id),-1) as date_id,
									coalesce(max(SITE_ID),-1) as SITE_ID,
									coalesce(max(first_session_activity),curr_hit.activity_dt) as first_session_activity
								from stg_opn_sess_lst_pg tmp
								where tmp.session_id = curr_pg.session_id
								into tmp_opn_pg;

--								end_tmstp := timeofday()::timestamptz;

--								SQL7 := SQL7 + (end_tmstp - start_tmstp);
--								SQLc7 := SQLc7 + 1::int8;

								IF tmp_opn_pg.page_id <> -1
									THEN			-- Extend a previous session
										session_start_tmstp := tmp_opn_pg.first_session_activity;
										session_page_count := tmp_opn_pg.page_sequence_id;
										new_sess_pre_pg := FALSE;
										IF (curr_hit.cleansed=1)
											THEN	-- Update the existing record, by clearing exit_page_flag and updating view_time.

--												start_tmstp := timeofday()::timestamptz;

												UPDATE web_site_activity_fa
												SET	exit_page_flag=(case when 1=0 then 1 else null end),
													view_time_sec=ceil(extract('epoch' from (curr_hit.activity_dt - (cast(get_date_desc(tmp_opn_pg.date_id)+get_time_desc(tmp_opn_pg.time_id) as timestamptz)))))
												WHERE (session_id = curr_pg.session_id)
												  AND (page_sequence_id = tmp_opn_pg.page_sequence_id);

--												end_tmstp := timeofday()::timestamptz;

--												SQL8 := SQL8 + (end_tmstp - start_tmstp);
--												SQLc8 := SQLc8 + 1::int8;

											ELSE	-- Load the existing record data into curr_pg, and mark the record for update
												page_record_exists := TRUE;
												curr_pg.page_id := tmp_opn_pg.page_id;
												curr_pg_start_tmstp := (cast(get_date_desc(tmp_opn_pg.date_id)+get_time_desc(tmp_opn_pg.time_id) as timestamptz));
												curr_pg.total_page_hits_cnt := tmp_opn_pg.total_page_hits;
												curr_pg.total_page_bytes := tmp_opn_pg.total_page_bytes;
												curr_pg.time_id := tmp_opn_pg.time_id;
												curr_pg.date_id := tmp_opn_pg.date_id;
												curr_pg.load_time_sec := tmp_opn_pg.load_time_sec;
												curr_pg.status_code_id := tmp_opn_pg.status_code_id;
												curr_pg.page_sequence_id := tmp_opn_pg.page_sequence_id;
												session_page_count := tmp_opn_pg.page_sequence_id;
										END IF;
									ELSE			-- No pages found in previous session
										session_extension_cnt := 0;	-- reset it so that we don't use it by accident.
										session_start_tmstp := curr_hit.activity_dt;
										session_page_count := 0;
								END IF;
							ELSE

--								msg_str := to_char(curr_pg.session_id,'999999999');
--								RAISE LOG 'Normal session: %', msg_str;

								session_extension_cnt := 0;	-- Just in case it was null
								session_start_tmstp := curr_hit.activity_dt;
								session_page_count := 0;
						END IF;
				END IF;

		END IF;									-- End of check for new session or page

		-- Finished processing new session and new page scenarios.  Update current page data.

		curr_pg_end_tmstp := curr_hit.activity_dt;
		session_end_tmstp := curr_hit.activity_dt;
		curr_pg.total_page_hits_cnt := curr_pg.total_page_hits_cnt + 1;
		curr_pg.total_page_bytes := curr_pg.total_page_bytes + curr_hit.bytes_sent;

		IF (curr_hit.cleansed=1)						-- Check for need to get page_id
			THEN								-- Need to get page_id for pages w/ cleansed url
				new_sess_pre_pg := FALSE;				-- After this, current page data is no longer pre-page
				curr_pg.status_code_id := curr_hit.status;
				cleaned_get_request := cleanGetRequest(curr_hit.get_request);
--				start_tmstp := timeofday()::timestamptz;

				BEGIN
                                       IF (last_server_name = curr_hit.server_name) 
                                       THEN
                                          curr_pg.SITE_ID = last_SITE_ID;
                                       ELSE
                                          BEGIN
					    select max(lu.SITE_ID) as SITE_ID, count(*) as fi_cnt
					      from DIM_SITE lu
					     where (coalesce(lu.domain_name,'NA') = coalesce(curr_hit.server_name,'NA'))
					      into curr_pg.SITE_ID, rec_ct;
 
                                            last_SITE_ID := curr_pg.SITE_ID;
                                            last_server_name := curr_hit.server_name;

				            IF ((curr_pg.SITE_ID is null) or (rec_ct = 0))		-- Check for no fi found
					    THEN
						curr_pg.SITE_ID := -1;
					 	RAISE WARNING 'Server not found in DIM_SITE.domain_name:  %',curr_hit.server_name;
				            END IF;

					  EXCEPTION
						WHEN OTHERS THEN
							curr_pg.SITE_ID := -1;
							RAISE WARNING 'Error identifying server:  %',curr_hit.server_name;
                                          END;
                                       END IF;
				END;


				BEGIN


					select max(lu.page_id) as page_id, count(*) as page_cnt
					from page_lu lu
					where (lu.url_string = cleaned_get_request)
					into curr_pg.page_id, rec_ct;

					EXCEPTION
						WHEN OTHERS THEN
							curr_pg.page_id := -1;
							RAISE WARNING 'Error identifying page:  %', curr_hit.get_request;
				END;

--				end_tmstp := timeofday()::timestamptz;

--				SQL9 := SQL9 + (end_tmstp - start_tmstp);
--				SQLc9 := SQLc9 + 1::int8;

				IF ((curr_pg.page_id is null) or (rec_ct = 0))		-- Check for no page found
					THEN
						curr_pg.page_id := -1;
						RAISE WARNING 'Page not found in page_lu.url_string:  %',curr_hit.get_request;
				END IF;

				If (rec_ct > 1)					-- Check for duplicate pages found
					THEN
						RAISE WARNING 'Duplicate page in page_lu.url_string:  %', curr_hit.get_request;
				END IF;

				If (curr_pg.page_id <> -1)				-- Valid page found.  Update session and page info.
					THEN
						session_page_count := session_page_count + 1;
						curr_pg.page_sequence_id := session_page_count;
				END IF;

		END IF;									-- End of check for need to get page_id

		-- Finished processing page update information.  Migrate current to previous.

		prev_pg.page_sequence_id 	:= curr_pg.page_sequence_id;
		prev_pg.page_id 		:= curr_pg.page_id;
		prev_pg.load_time_sec 		:= curr_pg.load_time_sec;
		prev_pg.total_page_hits_cnt 	:= curr_pg.total_page_hits_cnt;
--		prev_pg.exit_page_flag 		:= curr_pg.exit_page_flag;
		prev_pg.total_page_bytes 	:= curr_pg.total_page_bytes;
		prev_pg.view_time_sec 		:= curr_pg.view_time_sec;
		prev_pg.session_id 		:= curr_pg.session_id;
		prev_pg.status_code_id 		:= curr_pg.status_code_id;
		prev_pg.time_id 		:= curr_pg.time_id;
		prev_pg.date_id 		:= curr_pg.date_id;
		prev_pg.SITE_ID 			:= curr_pg.SITE_ID;

		prev_hit_temp_session_id := curr_hit.temp_session_id;
		prev_pg_start_tmstp := curr_pg_start_tmstp;
		prev_pg_end_tmstp := curr_pg_end_tmstp;
	END LOOP;

	IF ((prev_pg.page_id = -1) or (prev_pg.session_id = -1))			-- Check for last page and session are valid
		THEN									-- Invalid last page or session
			random_hits := random_hits + prev_pg.total_page_hits_cnt;
			random_bytes := random_bytes + prev_pg.total_page_bytes;
		ELSE 									-- Valid last page and session (write it)
			IF (prev_pg.load_time_sec < ceil(coalesce(extract('epoch' from (prev_pg_end_tmstp - prev_pg_start_tmstp)),0)))
				THEN
					prev_pg.load_time_sec := ceil(extract('epoch' from (prev_pg_end_tmstp - prev_pg_start_tmstp)));
			END IF;

			last_page := 1;

			IF (session_page_count < 2)
				THEN
					prev_pg.view_time_sec := 7;
				ELSE
					prev_pg.view_time_sec := ceil(extract('epoch' from (session_end_tmstp - session_start_tmstp)) / (session_page_count - 1));
			END IF;

			IF page_record_exists
				THEN

--					start_tmstp := timeofday()::timestamptz;

					UPDATE	web_site_activity_fa
					SET	load_time_sec=prev_pg.load_time_sec,
						total_page_hits_cnt=prev_pg.total_page_hits_cnt,
						view_time_sec=prev_pg.view_time_sec,
						total_page_bytes=prev_pg.total_page_bytes,
						exit_page_flag=last_page
					WHERE (session_id = prev_pg.session_id)
					  AND (page_sequence_id = prev_pg.page_sequence_id);

--					end_tmstp := timeofday()::timestamptz;

--					SQL10 := SQL10 + (end_tmstp - start_tmstp);
--					SQLc10 := SQLc10 + 1::int8;

				ELSE

--					start_tmstp := timeofday()::timestamptz;

					INSERT INTO web_site_activity_fa(
						page_sequence_id, page_id, time_id, date_id, 
						session_id, load_time_sec, total_page_hits_cnt, 
						view_time_sec, total_page_bytes, exit_page_flag, 
						status_code_id, SITE_ID)
					VALUES(	prev_pg.page_sequence_id, prev_pg.page_id, prev_pg.time_id, prev_pg.date_id, 
						prev_pg.session_id, prev_pg.load_time_sec, prev_pg.total_page_hits_cnt, 
						prev_pg.view_time_sec, prev_pg.total_page_bytes, last_page, 
						prev_pg.status_code_id, prev_pg.SITE_ID);

--					end_tmstp := timeofday()::timestamptz;

--					SQL11 := SQL11 + (end_tmstp - start_tmstp);
--					SQLc11 := SQLc11 + 1::int8;

			END IF;

			IF next_pg_ind = 0
				THEN	next_pg_a0 := prev_pg.page_id;
					next_pg_site_a0 := prev_pg.SITE_ID;
			ELSIF next_pg_ind = 1
				THEN	next_pg_a1 := prev_pg.page_id;
					next_pg_site_a1 := prev_pg.SITE_ID;
			ELSIF next_pg_ind = 2
				THEN	next_pg_a2 := prev_pg.page_id;
					next_pg_site_a2 := prev_pg.SITE_ID;
			ELSIF next_pg_ind = 3
				THEN	next_pg_a3 := prev_pg.page_id;
					next_pg_site_a3 := prev_pg.SITE_ID;
			ELSIF next_pg_ind = 4
				THEN	next_pg_a4 := prev_pg.page_id;
					next_pg_site_a4 := prev_pg.SITE_ID;
			ELSIF next_pg_ind > 4
				THEN
					next_pg_ind := 4;
--					start_tmstp := timeofday()::timestamptz;

					INSERT INTO tmp_clickstream_next (page_id, page_1_id, page_2_id, page_3_id, page_4_id, date_id, SITE_ID)
					VALUES (next_pg_a0,next_pg_a1,next_pg_a2,next_pg_a3,next_pg_a4,prev_pg.date_id,next_pg_site_a0);

--					end_tmstp := timeofday()::timestamptz;

--					SQL12 := SQL12 + (end_tmstp - start_tmstp);
--					SQLc12 := SQLc12 + 1::int8;

				  -- Shift to next page in clickstream

					next_pg_a0 := next_pg_a1;
					next_pg_a1 := next_pg_a2;
					next_pg_a2 := next_pg_a3;
					next_pg_a3 := next_pg_a4;
					next_pg_a4 := prev_pg.page_id;

					next_pg_site_a0 := next_pg_site_a1;
					next_pg_site_a1 := next_pg_site_a2;
					next_pg_site_a2 := next_pg_site_a3;
					next_pg_site_a3 := next_pg_site_a4;
					next_pg_site_a4 := prev_pg.SITE_ID;

			END IF;

			next_pg_ind := next_pg_ind + 1;

			prev_pg_a4 := prev_pg_a3;
			prev_pg_a3 := prev_pg_a2;
			prev_pg_a2 := prev_pg_a1;
			prev_pg_a1 := prev_pg_a0;
			prev_pg_a0 := prev_pg.page_id;

--			start_tmstp := timeofday()::timestamptz;

			INSERT INTO tmp_clickstream_prev (page_id, prev_page_1_id, prev_page_2_id, prev_page_3_id, prev_page_4_id, date_id, SITE_ID)
			VALUES (prev_pg_a0,prev_pg_a1,prev_pg_a2,prev_pg_a3,prev_pg_a4,prev_pg.date_id,prev_pg.SITE_ID);

--			end_tmstp := timeofday()::timestamptz;

--			SQL13 := SQL13 + (end_tmstp - start_tmstp);
--			SQLc13 := SQLc13 + 1::int8;

	END IF;										-- End of Check for last page and session are valid

  -- Finish writing next_page clickstream entries for this session

	FOR i IN 1..next_pg_ind LOOP
	  -- Write clickstream to database

--		start_tmstp := timeofday()::timestamptz;

		INSERT INTO tmp_clickstream_next (page_id, page_1_id, page_2_id, page_3_id, page_4_id, date_id, SITE_ID)
		VALUES (next_pg_a0,next_pg_a1,next_pg_a2,next_pg_a3,next_pg_a4,prev_pg.date_id,next_pg_site_a0);

--		end_tmstp := timeofday()::timestamptz;

--		SQL14 := SQL14 + (end_tmstp - start_tmstp);
--		SQLc14 := SQLc14 + 1::int8;

	  -- Shift to next page in clickstream

		next_pg_a0 := next_pg_a1;
		next_pg_a1 := next_pg_a2;
		next_pg_a2 := next_pg_a3;
		next_pg_a3 := next_pg_a4;
		next_pg_a4 := no_pg;

		next_pg_site_a0 := next_pg_site_a1;
		next_pg_site_a1 := next_pg_site_a2;
		next_pg_site_a2 := next_pg_site_a3;
		next_pg_site_a3 := next_pg_site_a4;
		next_pg_site_a4 := no_site;

	END LOOP;

	IF (random_hits > 0)								-- Check for random hits
		THEN
--			msg_str := 'date_id='||coalesce(trim(to_char(get_date_id(session_end_tmstp),'999999')),'null')||
--					', random_hits='||coalesce(trim(to_char(random_hits,'999999999999')),'null')||
--					', random_bytes='||coalesce(trim(to_char(random_bytes,'999999999999')),'null');
--			RAISE NOTICE 'Random Hits: %',msg_str;

--			start_tmstp := timeofday()::timestamptz;

			BEGIN

				INSERT INTO web_site_activity_fa(
					page_sequence_id, page_id, time_id, date_id, session_id, load_time_sec, total_page_hits_cnt, 
					view_time_sec, total_page_bytes, exit_page_flag, status_code_id, SITE_ID)
				VALUES(	0, -1, 0, get_date_id(session_end_tmstp),-1, 0, random_hits, 
					0, random_bytes, null, -1, -1);
				EXCEPTION
					WHEN OTHERS THEN
						msg_str := 'date_id='||coalesce(trim(to_char(get_date_id(session_end_tmstp),'999999')),'null')||
							', random_hits='||coalesce(trim(to_char(random_hits,'999999999999')),'null')||
							', random_bytes='||coalesce(trim(to_char(random_bytes,'999999999999')),'null');
						RAISE WARNING 'Unable to write random hit data: %',msg_str;
			END;

--			end_tmstp := timeofday()::timestamptz;

--			SQL15 := SQL15 + (end_tmstp - start_tmstp);
--			SQLc15 := SQLc15 + 1::int8;

	END IF;										-- End of Check for random hits

--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 1, SQLc1, SQL1, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 2, SQLc2, SQL2, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 3, SQLc3, SQL3, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 4, SQLc4, SQL4, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 5, SQLc5, SQL5, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 6, SQLc6, SQL6, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 7, SQLc7, SQL7, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 8, SQLc8, SQL8, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 9, SQLc9, SQL9, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 10, SQLc10, SQL10, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 11, SQLc11, SQL11, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 12, SQLc12, SQL12, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 13, SQLc13, SQL13, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 14, SQLc14, SQL14, partition_id, job_id);
--				insert into public.diagnose_queries (statement_no, runs, total_time, part_id, jobid) values( 15, SQLc15, SQL15, partition_id, job_id);
	
	RETURN (TRUE);
  END;

$$
    LANGUAGE plpgsql;



--
-- TOC entry 43 (class 1255 OID 17533)
-- Dependencies: 11
-- Name: plpgsql_call_handler(); Type: FUNCTION; Schema: WebClickStream; Owner: postgres
--

CREATE FUNCTION plpgsql_call_handler() RETURNS language_handler
    AS '$libdir/plpgsql', 'plpgsql_call_handler'
    LANGUAGE c;


--
-- TOC entry 45 (class 1255 OID 17534)
-- Dependencies: 11
-- Name: plpgsql_validator(oid); Type: FUNCTION; Schema: WebClickStream; Owner: postgres
--

CREATE FUNCTION plpgsql_validator(oid) RETURNS void
    AS '$libdir/plpgsql', 'plpgsql_validator'
    LANGUAGE c;


--
-- TOC entry 47 (class 1255 OID 17535)
-- Dependencies: 11 375
-- Name: trunc_table(text); Type: FUNCTION; Schema: WebClickStream; Owner: postgres
--

CREATE FUNCTION trunc_table(table_name text) RETURNS boolean
    AS $$
begin
	EXECUTE 'TRUNCATE ' || table_name;
	RETURN TRUE;
exception
	WHEN OTHERS THEN
		RETURN FALSE;
end; $$
    LANGUAGE plpgsql STRICT SECURITY DEFINER;


--
-- TOC entry 50 (class 1255 OID 341391084)
-- Dependencies: 11 375
-- Name: trunc_wsaf_stg(); Type: FUNCTION; Schema: WebClickStream; Owner: etl
--

CREATE FUNCTION trunc_wsaf_stg() RETURNS boolean
    AS $$
  DECLARE
	min_dt int4;
	upd_dt int4;
  BEGIN

	-- Determine if the first day of the week has new data.

	select date_id, week_only from stg_date_list order by date_id limit 1
	into min_dt, upd_dt;

	IF upd_dt=0	
		THEN	-- First day of the week is new data.  Can truncate stg_wsaf_agg.
			truncate stg_wsaf_agg;
		ELSE	-- First day of the week has old data.  Keep valid data, and delete invalid data.
			delete from stg_wsaf_agg
			where date_id not in (select distinct lst.date_id from stg_date_list lst where lst.week_only = 2);
	END IF;

	RETURN TRUE;
  END;
$$
    LANGUAGE plpgsql;

-- gets the latest load date from the daily agg, if empty defaults to current date
CREATE OR REPLACE VIEW max_load_date_lu as
select 'Daily' as data_interval, date_desc from (select coalesce(max(date_id),get_date_id(current_date)) as date_id from dy_st_agg) as x join date_lu using(date_id)
union
select 'Weekly',date_desc - interval '1 day' * (day_of_week_nbr-1) from (select coalesce(max(date_id),get_date_id(current_date)) as date_id from dy_st_agg) as x join date_lu using(date_id);





alter table day_clickstream_prev owner to etl;
alter table file_lu owner to etl;
alter table tmp_hit_hourly_a owner to etl;
alter table tmp_hit_hourly_b owner to etl;
alter table pmt_tmp_hit_hourly owner to etl;
alter table pmt_tmp_session_hourly owner to etl;
alter table stg_files_to_monitor owner to etl;
alter table temp_session_referrer_url owner to etl;
alter table tmp_clickstream_next owner to etl;
alter table tmp_clickstream_prev owner to etl;
alter table tmp_downloads owner to etl;
alter table tmp_hit owner to etl;
alter table tmp_hit_subset owner to etl;
alter table tmp_session_hourly_a owner to etl;
alter table tmp_session_hourly_b owner to etl;
alter table browser_lu owner to etl;
alter table date_lu owner to etl;
alter table day_hit_subset_agg owner to etl;
alter table DIM_SITE owner to etl;
alter table dy_fl_stat_part owner to etl;
alter table html_status_code_lu owner to etl;
alter table geography_lu owner to etl;
alter table ip_address_lu owner to etl;
alter table page_lu owner to etl;
alter table session_lu owner to etl;
alter table stg_date_list owner to etl;
alter table stg_opn_sess_lst_pg owner to etl;
alter table stg_open_session owner to etl;
alter table stg_pers_id_list owner to etl;
alter table stg_sess_agg owner to etl;
alter table time_of_day_lu owner to etl;
alter table day_clickstream_next owner to etl;
alter table dy_br_vmg_agg owner to etl;
alter table dy_br_vmg_pvg_agg owner to etl;
alter table dy_st_agg owner to etl;
alter table tmp_session owner to etl;
alter table tmp_new_session owner to etl;
alter table stg_wsaf_agg_1 owner to etl;
alter table stg_wsaf_agg owner to etl;
alter table web_site_activity_fa owner to etl;
alter table wk_br_hrg_agg owner to etl;
alter table wk_hrg_agg owner to etl;
alter table wk_hrg_st_agg owner to etl;
alter table wk_ip_agg owner to etl;
alter table wk_pg_br_stat_agg owner to etl;
alter table wk_pg_expg_part owner to etl;
alter table wk_pg_seq_part owner to etl;
alter table wk_pvg_agg owner to etl;
alter table wk_ref_agg owner to etl;
alter table wk_vmg_agg owner to etl;
alter table wk_vmg_st_agg owner to etl;
alter table hit_subset_fa owner to etl;



INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (-1, 'NA');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (100, 'Continue');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (101, 'Switching Protocols');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (200, 'OK');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (201, 'Created');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (202, 'Accepted');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (203, 'Partial Information');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (204, 'No Content');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (205, 'Reset Content');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (206, 'Partial Content');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (300, 'Multiple Choices');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (301, 'Moved Permanently');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (302, 'Moved Temporarily');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (303, 'See Other');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (304, 'Not Modified');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (305, 'Use Proxy');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (400, 'Bad Request');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (401, 'Unauthorized');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (402, 'Payment Required');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (404, 'Not Found');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (405, 'Method not allowed');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (406, 'Not acceptible');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (407, 'Proxy authentication required');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (408, 'Request Time-out');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (409, 'Conflict');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (410, 'Gone');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (411, 'Length Required');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (412, 'Precondition Failed');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (413, 'Request entity to large');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (414, 'Request URL to large');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (415, 'Unsupported media type');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (500, 'Server error');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (501, 'Not Implemented');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (502, 'Bad Gateway');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (503, 'Out of resources');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (504, 'Gateway Time-out');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (505, 'HTTP version not supported');
INSERT INTO html_status_code_lu (status_code_id, status_code_desc) VALUES (307, 'Temporary Redirect');

GRANT ALL ON TABLE idgenerator TO GROUP dwh;
GRANT ALL ON TABLE seq_browser_id TO GROUP dwh;
GRANT ALL ON TABLE seq_file_id TO GROUP dwh;
GRANT ALL ON TABLE seq_site_id TO GROUP dwh;
GRANT ALL ON TABLE seq_page_id TO GROUP dwh;
GRANT ALL ON TABLE seq_session_id TO GROUP dwh;

GRANT SELECT ON TABLE hr_sess_dy_part_a TO GROUP dwh;
GRANT SELECT ON TABLE hr_sess_dy_part_b TO GROUP dwh;
GRANT SELECT ON TABLE month_year_lu TO GROUP dwh;
GRANT SELECT ON TABLE page_paths_lu TO GROUP dwh;
GRANT SELECT ON TABLE path_1_lu TO GROUP dwh;
GRANT SELECT ON TABLE pmt_hr_sess_dy_part TO GROUP dwh;
GRANT SELECT ON TABLE referrer_lu TO GROUP dwh;
GRANT SELECT ON TABLE referrer_url_lu TO GROUP dwh;
GRANT SELECT ON TABLE session_fa TO GROUP dwh;
GRANT SELECT ON TABLE url_path_1_lu TO GROUP dwh;
GRANT SELECT ON TABLE url_path_2_lu TO GROUP dwh;
GRANT SELECT ON TABLE url_path_3_lu TO GROUP dwh;
GRANT SELECT ON TABLE url_path_4_lu TO GROUP dwh;
GRANT SELECT ON TABLE week_lu TO GROUP dwh;
GRANT SELECT ON TABLE week_of_year_lu TO GROUP dwh;
GRANT SELECT ON TABLE file_type_lu TO GROUP dwh;
GRANT SELECT ON TABLE max_load_date_lu TO GROUP dwh;
