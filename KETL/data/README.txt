Data Directory Structure
------------------------

staging-daily:
  Weblogs must be dropped here, when all files are dropped a file
  must be created to signify that all logs have been dropped. Currently all
  files are expected to be compressed using gzip.

processing-daily:
  Location of files whilst being loaded into TMP_HIT and
  TMP_SESSION.

complete:
  Location of files after they have been loaded. It is the users
  responsibility to archive these files.

Log file naming and touch file naming.
--------------------------------------

The scripts found in $KETLDIR/scripts are configured to search for files
matching the follow search patterns, modification of these search patterns is
relatively simple and in the future it would be ideal to make these exist in
the metadata only.

Touch file search string: 2???????.??

Log file search string: access.e*.2???????.??.gz

If the search strings need to be changed modify the following files.
1. dailyFileHandler.sh
2. postLoadCompress.sh

and modify the parameter SEARCHPATH of the parameter list myLogFiles to
contain the path and search string of the log files when they reside in the
processing-daily directory.

e.g. if the search string for the uncompressed log files was weblogs*.?? the
     value of SEARCHPATH would become:
        /home/etl/KETL/data/processing-daily/weblogs*.??
     however it could also just be set to:
        /home/etl/KETL/data/processing-daily/*
     but this is could result in unexpected errors if other files ended up in
     the processing-daily directory.

