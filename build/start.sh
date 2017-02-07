#!/bin/bash
chown -R postgres var/lib/postgresql/
chown -R postgres var/log/postgresql/
chown -R postgres etc/postgresql

su postgres -c '/etc/init.d/postgresql stop'
su postgres -c '/etc/init.d/postgresql start'

./cobradb/bin/load_db --drop-all
