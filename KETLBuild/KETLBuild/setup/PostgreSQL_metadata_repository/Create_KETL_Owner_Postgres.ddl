CREATE USER ketlmd
  NOCREATEDB NOCREATEUSER PASSWORD 'ketlmd';

ALTER USER ketlmd SET search_path=ketlmd,public,pg_catalog;
