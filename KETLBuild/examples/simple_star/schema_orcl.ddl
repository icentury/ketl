
DROP TABLE sales_fact CASCADE CONSTRAINTS;

CREATE TABLE sales_fact (
       order_surrogate_key  NUMBER(9) NOT NULL,
       product_surrogate_key NUMBER(9) NOT NULL,
       date_surrogate_key   NUMBER(9) NOT NULL,
       customer_surrogate_key NUMBER(9) NOT NULL,
       unit_price           NUMBER(9,2) NULL,
       quanity_sold         INTEGER NULL
);


ALTER TABLE sales_fact
       ADD PRIMARY KEY (order_surrogate_key, product_surrogate_key, 
              date_surrogate_key, customer_surrogate_key);


DROP TABLE product_dimension CASCADE CONSTRAINTS;

CREATE TABLE product_dimension (
       product_surrogate_key NUMBER(9) NOT NULL,
       effective_timestamp  DATE NOT NULL,
       product_natural_key  VARCHAR2(20) NULL,
       expiration_timestamp DATE NOT NULL,
       name                 VARCHAR2(20) NULL,
       category             VARCHAR2(20) NULL,
       line                 VARCHAR2(20) NULL
);


ALTER TABLE product_dimension
       ADD PRIMARY KEY (product_surrogate_key);


DROP TABLE customer_dimension CASCADE CONSTRAINTS;

CREATE TABLE customer_dimension (
       customer_surrogate_key NUMBER(9) NOT NULL,
       effective_timestamp  DATE NOT NULL,
       customer_natural_key VARCHAR2(20) NULL,
       expiration_timestamp DATE NULL,
       first_name           VARCHAR2(20) NULL,
       last_name            VARCHAR2(20) NULL,
       date_of_birth        DATE NULL
);


ALTER TABLE customer_dimension
       ADD PRIMARY KEY (customer_surrogate_key);


DROP TABLE date_dimension CASCADE CONSTRAINTS;

CREATE TABLE date_dimension (
       date_surrogate_key   NUMBER(9) NOT NULL,
       date_description     DATE NULL,
       day_of_month         INTEGER NULL,
       day_of_week          INTEGER NULL,
       day_of_year          INTEGER NULL,
       month_of_year        INTEGER NULL,
       year                 INTEGER NULL
);


ALTER TABLE date_dimension
       ADD PRIMARY KEY (date_surrogate_key);


DROP TABLE order_dimension CASCADE CONSTRAINTS;

CREATE TABLE order_dimension (
       order_surrogate_key  NUMBER(9) NOT NULL,
       order_natural_key    VARCHAR2(20) NULL,
       shipped_date         DATE NULL,
       returned_date        DATE NULL
);


ALTER TABLE order_dimension
       ADD PRIMARY KEY (order_surrogate_key);


ALTER TABLE sales_fact
       ADD FOREIGN KEY (customer_surrogate_key)
                             REFERENCES customer_dimension  (
              customer_surrogate_key);


ALTER TABLE sales_fact
       ADD FOREIGN KEY (date_surrogate_key)
                             REFERENCES date_dimension  (
              date_surrogate_key);


ALTER TABLE sales_fact
       ADD FOREIGN KEY (product_surrogate_key)
                             REFERENCES product_dimension  (
              product_surrogate_key);


ALTER TABLE sales_fact
       ADD FOREIGN KEY (order_surrogate_key)
                             REFERENCES order_dimension  (
              order_surrogate_key);



