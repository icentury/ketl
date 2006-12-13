# Description: Handles weblogs, putting the most recent weblogs
#              into the processeing directory and all the others into the
#              complete directory
#              Looks for correct number of files in data-directory, touch file
#              when done, compressed the file in the complete directory
# 
#  Touch file format must be 2???????.?? - used to show all files dropped
#  Log file format access*.$WEBLOG_EXT, which is 2??????? and matches
#  touch file.

if [ -z "$2" ]
then
  echo "Usage: `basename $0` data-directory number-of-files-to-expect"
  exit 1
fi

DATADIR=$1
# set the staging directory
STAGING_DAILY=$DATADIR/staging-daily

# set the processing 
PROCESSING_DIR=$DATADIR/processing-daily

# completed welogs drop directory
COMPLETE_DIR=$DATADIR/complete

# set the number of expected files
EXPECTED_FILE_COUNT=$2

# set indicator file to blank 
FILES_FOUND="NO"


# look through each indicator file checking for a complete set of logs 
for i in $( ls $STAGING_DAILY/2???????.?? ); do
  
     # strip to filename only and use for weblog
     WEBLOG_EXT=${i##*/}
     WEBLOG_EXT=${WEBLOG_EXT:0:8}
     # get a file count
     FOUND_FILE_COUNT=`ls $STAGING_DAILY/access*.$WEBLOG_EXT | wc -l`

     # record files found
     FILES_FOUND="YES"

     # check file count and if ok move to processing
     if [ $FOUND_FILE_COUNT -ge  $EXPECTED_FILE_COUNT ]; then
        TMP=`ls $STAGING_DAILY/access*.$WEBLOG_EXT`
        MV_CMD="$MV_CMD $TMP $i" 
     else
        echo "Wrong number of files found" $FOUND_FILE_COUNT "expected" $EXPECTED_FILE_COUNT "for the" $WEBLOG_EXT
        exit 1      
     fi
done

if [ $FILES_FOUND == "NO" ]; then
   echo "No daily files found"
   # exit code 99 prevents emails from being sent out
   exit 99
fi


# mv the processed logs to another directory
for x in $(ls $PROCESSING_DIR/2???????.?? 2> /dev/null); do
    PROC_EXT=${x##*/}
    mkdir $COMPLETE_DIR/$PROC_EXT 2> /dev/null
    mv -f $PROCESSING_DIR/*$PROC_EXT $COMPLETE_DIR/$PROC_EXT 2> /dev/null
    FNL_EXT=${PROC_EXT:0:8}
    mv -f $PROCESSING_DIR/*$FNL_EXT $COMPLETE_DIR/$PROC_EXT 2> /dev/null
    gzip $COMPLETE_DIR/$PROC_EXT/acce* 2> /dev/null
done

# move the new files in for processing
mv -f $MV_CMD $PROCESSING_DIR

exit 0 



