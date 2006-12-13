 
CREATE Group dwh;
  
CREATE USER ketlmd
  NOCREATEDB NOCREATEUSER PASSWORD 'ketlmd';

CREATE USER etl
  NOCREATEDB NOCREATEUSER PASSWORD 'etl';

ALTER USER ketlmd SET search_path=ketlmd,public,pg_catalog;

ALTER USER etl SET search_path=WebClickStream, public;

ALTER GROUP dwh ADD USER etl;

