
if [ -z "$1" ]
then
  echo "Usage: `basename $0` data-directory "
  exit 1
fi

DATADIR=$1

# can be used to fire of anything, eg create touch file for another system
# touch $DATADIR/flags/dailyComplete
