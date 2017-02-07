#!/bin/bash

chown -R postgres:postgres var/lib/postgresql/
chown -R postgres:postgres var/log/postgresql/
chown -R postgres:postgres etc/postgresql

su postgres -c '/etc/init.d/postgresql stop'
su postgres -c '/etc/init.d/postgresql start'

