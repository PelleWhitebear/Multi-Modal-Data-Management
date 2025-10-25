#!/bin/bash
set -e
echo "Running the full data pipeline..."
echo "- - - - - STARTING LANDING ZONE - - - - -"
t0=$(date +%s)
bash landing_zone/landing_zone.sh
t1=$(date +%s)
echo "- - - - - LANDING ZONE COMPLETE - - - - -"

echo "- - - - - STARTING FORMATTED ZONE - - - - -"
t2=$(date +%s)
bash formatted_zone/formatted_zone.sh
t3=$(date +%s)
echo "- - - - - FORMATTED ZONE COMPLETE - - - - -"

echo "- - - - - STARTING TRUSTED ZONE - - - - -"
t4=$(date +%s)
bash trusted_zone/trusted_zone.sh
t5=$(date +%s)
echo "- - - - - TRUSTED ZONE COMPLETE - - - - -"

echo "- - - - - STARTING EXPLOITATION ZONE - - - - -"
t6=$(date +%s)
bash exploitation_zone/exploitation_zone.sh
t7=$(date +%s)
echo "- - - - - EXPLOITATION ZONE COMPLETE - - - - -"

elapsed=$(( t7 - t0 ))
echo "Time taken for the landing zone: $((t1 - t0)) seconds."
echo "Time taken for the formatted zone: $((t3 - t2)) seconds."
echo "Time taken for the trusted zone: $((t5 - t4)) seconds."
echo "Time taken for the exploitation zone: $((t7 - t6)) seconds."
echo "Total time taken: $elapsed seconds."