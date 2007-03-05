# Description: Handles weblogs, looks for complete file in staging
#              if found, it deletes it and moves all other files
#              to processing. Files in processing already are moved
#              to complete

if [ -z "$1" ]
then
  echo "Usage: `basename $0` data-directory"
  exit 1
fi

DATADIR=$1
# set the staging directory
STAGING_DAILY=$DATADIR/staging-daily

# set the processing 
PROCESSING_DIR=$DATADIR/processing-daily

# completed welogs drop directory
COMPLETE_DIR=$DATADIR/complete


# set indicator file to blank 
FILES_FOUND="NO"


# mv the processed logs to another directory
mv  $PROCESSING_DIR/* $COMPLETE_DIR/ 2> /dev/null


# look through each indicator file checking for a complete set of logs 
for i in $( ls $STAGING_DAILY/complete ); do
       
     # record files found
     FILES_FOUND="YES"

     # remove complete flag and move to processing
     rm -f $STAGING_DAILY/complete
     mv $DATADIR/staging-daily/* $PROCESSING_DIR
done

if [ $FILES_FOUND == "NO" ]; then
   echo "No daily files found"
   # exit code 99 prevents emails from being sent out
   exit 99
fi


exit 0 



