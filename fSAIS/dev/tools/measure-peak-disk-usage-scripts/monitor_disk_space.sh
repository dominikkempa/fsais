#!/bin/sh
while [ true ]
do
	df -m | grep '/dev/sda' | awk '{ print $4 }'
	sleep 3
done
