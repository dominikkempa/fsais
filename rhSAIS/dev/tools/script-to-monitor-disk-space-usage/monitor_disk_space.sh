#!/bin/sh
while [ true ]
do
	df -m | grep ' /home$' | awk '{ print $4 }'
	sleep 3
done
