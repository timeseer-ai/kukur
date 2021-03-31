#!/bin/bash

influx -import -compressed -path=/NOAA_data.txt.gz -precision=s -database=NOAA_water_database
