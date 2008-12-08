ALTER TABLE SERVER_EXECUTOR
ADD pool text;

update SERVER_EXECUTOR set pool = 'Default';

ALTER TABLE JOB
ADD pool text;
