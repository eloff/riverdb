#!/usr/bin/env bash

DB=${1:-riverdb_test}

export PGPASSWORD='1234'
psql "host=127.0.0.1 dbname=${DB} user=riverdb_test sslmode=disable"