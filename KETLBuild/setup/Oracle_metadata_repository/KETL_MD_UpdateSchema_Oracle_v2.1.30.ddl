ALTER TABLE SERVER_EXECUTOR
  add (pool varchar2(50));

update SERVER_EXECUTOR set pool = 'Default';

ALTER TABLE JOB
  add (pool varchar2(50));
