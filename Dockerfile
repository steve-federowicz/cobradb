FROM continuumio/anaconda3
MAINTAINER Steve Federowicz <sfederow@gmail.com>

# Add the PostgreSQL PGP key to verify their Debian packages.
RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8

# Add PostgreSQL's repository. It contains the most recent stable release
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y python-software-properties software-properties-common postgresql-server-dev-9.6 postgresql-client-9.6 postgresql-contrib-9.6 vim gcc

USER postgres

RUN /etc/init.d/postgresql start && \
    psql --command "CREATE USER cobra_user WITH SUPERUSER PASSWORD '';" && \
    psql --command "CREATE USER root WITH SUPERUSER PASSWORD '';" && \
    createdb -O cobra_user cobradb

# Adjust PostgreSQL configuration so that remote connections to the
# database are possible.
RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/9.6/main/pg_hba.conf

# And add ``listen_addresses`` to ``/etc/postgresql/9.6/main/postgresql.conf``
RUN echo "listen_addresses='*'" >> /etc/postgresql/9.6/main/postgresql.conf

# Expose the PostgreSQL port
EXPOSE 5432

USER root

RUN git clone https://github.com/steve-federowicz/ome.git cobradb

RUN pip install ./cobradb 

RUN cd bin && \
    ./load_db --drop-all

CMD bash -C '/bin/bash'

