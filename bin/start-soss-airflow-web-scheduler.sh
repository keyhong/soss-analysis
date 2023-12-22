#!/usr/bin/env bash

nohup airflow webserver 1> /dev/null 2>&1 & 
nohup airflow scheduler 1> /dev/null 2>&1 &
