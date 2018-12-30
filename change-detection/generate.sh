#!/bin/sh

[[ $# != 2 && ! "$2" =~ ^[0-9]+$ ]] && {
	echo "Usage:\n\t$0 <file> <number>\nwhere <number> - count of records to generate"
	exit 13
}
FILE=$1
MAX=$2
echo "Generating $MAX records into $FILE"
echo "id:name:timestamp:text" > $FILE
for i in $(seq $MAX); do
	echo "$i:element$i:1:text $i" >> $FILE
done
