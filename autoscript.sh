#!/bin/bash
# Este es nuestro primer progrma
echo -= auto update script =- 

WORKING_DIR="/var/www/html/"
DATAFILE="https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.csv"
DATAFILE_INF="datafile.inf"
DATAFILE_INF_OLD="datafile.inf.old"
LINETOCHECK="4"
DBUSER="root"
DBPASSWORD="password"

checkForUpdates(){
	cmd="curl --head -o $DATAFILE_INF $DATAFILE "
	RESPONSE=`$cmd`
	# compare data source with the last data source processed
	n=0
	currentDate=""
	while read line; do
		n=$((n+1))
		# reading each line
		if [ "$n" -eq "$LINETOCHECK" ] 
		then
			currentDate=$line
		fi
	done < $DATAFILE_INF
	echo "Current $currentDate"
	if test -f "$DATAFILE_INF_OLD"; then
		previousDate=""
		n=0
		while read line; do
			n=$((n+1))
			# reading each line
			if [ "$n" -eq "$LINETOCHECK" ] 
			then
				previousDate=$line
			fi
		done < $DATAFILE_INF_OLD
		echo "Previous $previousDate"
		if [ "$currentDate" == "$previousDate" ]; then
			return 1
		fi
	fi
	return 0
}

# get data source file header
if ! cd "$WORKING_DIR"; then
	echo "ERROR: can't access working directory ($WORKING_DIR)" >&2
	exit 1
fi
if ! checkForUpdates; then
	echo ">>> Nothing to update"
	exit 1
fi
echo "Downloading"
rm $DATAFILE
cmd="wget $DATAFILE"
RESPONSE=`$cmd`
echo "Loading"
mysql --user="$DBUSER" --password="$DBPASSWORD" --local-infile mapa < update.sql 
# Confirm data updated
mv $DATAFILE_INF $DATAFILE_INF_OLD
echo "Generating JSON files"
python consulta.py
