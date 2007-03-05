--
-- PostgreSQL database dump
--

-- Started on 2006-12-11 15:36:51 Pacific Standard Time

SET client_encoding = 'SQL_ASCII';
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 7 (class 2615 OID 83683319)
-- Name: simple_star; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA simple_star;


ALTER SCHEMA simple_star OWNER TO postgres;

SET search_path = simple_star, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 1278 (class 1259 OID 83683337)
-- Dependencies: 7
-- Name: customer_dimension; Type: TABLE; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE TABLE customer_dimension (
    customer_surrogate_key numeric(9,0),
    effective_timestamp date,
    customer_natural_key character varying(20),
    expiration_timestamp date,
    first_name character varying(20),
    last_name character varying(20),
    date_of_birth date
);


ALTER TABLE simple_star.customer_dimension OWNER TO simple_star;

--
-- TOC entry 1279 (class 1259 OID 83683340)
-- Dependencies: 7
-- Name: date_dimension; Type: TABLE; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE TABLE date_dimension (
    date_surrogate_key numeric(9,0),
    date_description date,
    day_of_month integer,
    day_of_week integer,
    day_of_year integer,
    month_of_year integer,
    "year" integer
);


ALTER TABLE simple_star.date_dimension OWNER TO simple_star;

--
-- TOC entry 1280 (class 1259 OID 83683343)
-- Dependencies: 7
-- Name: order_dimension; Type: TABLE; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE TABLE order_dimension (
    order_surrogate_key numeric(9,0),
    order_natural_key character varying(20),
    shipped_date date,
    returned_date date
);


ALTER TABLE simple_star.order_dimension OWNER TO simple_star;

--
-- TOC entry 1277 (class 1259 OID 83683334)
-- Dependencies: 7
-- Name: product_dimension; Type: TABLE; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE TABLE product_dimension (
    product_surrogate_key numeric(9,0),
    effective_timestamp date,
    product_natural_key character varying(20),
    expiration_timestamp date,
    name character varying(20),
    category character varying(20),
    line character varying(20)
);


ALTER TABLE simple_star.product_dimension OWNER TO simple_star;

--
-- TOC entry 1276 (class 1259 OID 83683331)
-- Dependencies: 7
-- Name: sales_fact; Type: TABLE; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE TABLE sales_fact (
    order_surrogate_key numeric(9,0),
    product_surrogate_key numeric(9,0),
    date_surrogate_key numeric(9,0),
    customer_surrogate_key numeric(9,0),
    unit_price numeric(9,2),
    quanity_sold integer
);


ALTER TABLE simple_star.sales_fact OWNER TO simple_star;

--
-- TOC entry 1605 (class 1259 OID 83683339)
-- Dependencies: 1278
-- Name: xpkcustomer_dimension; Type: INDEX; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE UNIQUE INDEX xpkcustomer_dimension ON customer_dimension USING btree (customer_surrogate_key);


--
-- TOC entry 1606 (class 1259 OID 83683342)
-- Dependencies: 1279
-- Name: xpkdate_dimension; Type: INDEX; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE UNIQUE INDEX xpkdate_dimension ON date_dimension USING btree (date_surrogate_key);


--
-- TOC entry 1607 (class 1259 OID 83683345)
-- Dependencies: 1280
-- Name: xpkorder_dimension; Type: INDEX; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE UNIQUE INDEX xpkorder_dimension ON order_dimension USING btree (order_surrogate_key);


--
-- TOC entry 1604 (class 1259 OID 83683336)
-- Dependencies: 1277
-- Name: xpkproduct_dimension; Type: INDEX; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE UNIQUE INDEX xpkproduct_dimension ON product_dimension USING btree (product_surrogate_key);


--
-- TOC entry 1603 (class 1259 OID 83683333)
-- Dependencies: 1276 1276 1276 1276
-- Name: xpksales_fact; Type: INDEX; Schema: simple_star; Owner: simple_star; Tablespace: 
--

CREATE UNIQUE INDEX xpksales_fact ON sales_fact USING btree (order_surrogate_key, product_surrogate_key, date_surrogate_key, customer_surrogate_key);


-- Completed on 2006-12-11 15:36:51 Pacific Standard Time

--
-- PostgreSQL database dump complete
--

