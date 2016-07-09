#!/bin/bash
echo "script out 0" > "script0.out"
echo "######################################"
echo "#### Launching container script 0 ####"
echo "######################################"
echo ""

echo "Listing files in current working directory:"
ls -la

echo ""
echo "Running:"

while [ 1 ]
do
	date
	sleep 1
done
