# immudb-audit

immudb-audit is a simple service and cli tool to store json formatted input and audit it later in immudb key-value or SQL.

## Overview
immudb-audit 

### Storing data



### Reading and auditing data

## Storing pgaudit logs in immudb
[pgaudit](https://github.com/pgaudit/pgaudit) is PostgreSQL extension that enables audit logs for the database. Any kind of audit logs should be stored in secure location. immudb is fullfiling this requirement with its immutable and tamper proof features.

immudb-audit has simple pgaudit log line parser. It assumes that each log line has log_line_prefix of '%m [%p] '.

To start, you need to have an PostgreSQL running with pgaudit extension enabled. As the example, [bitnami postgresql](https://hub.docker.com/r/bitnami/postgresql) which already hase pgaudi extension can be used. 

### Start PostgreSQL

You can use [docker-compose.yml](test/pgaudit/docker-compose.yml) from this repository as an example.

```bash
docker-compose up -d -f test/pgaudit/docker-compose.yml
```

### Create immudb-audit collection for logs

```bash
./immudb-play create kv pgaudit --parser pgaudit
```

### Tail PostgreSQL docker container logs

```bash
./immudb-play tail docker pgaudit psql-postgresql-1 --stdout --stderr --follow
```

Optionally, adding --log-level trace will print out all lines parsed and stored. 

### Read

```bash
./immudb-play read kv pgaudit statement_id=100
./immudb-play read kv pgaudit command=INSERT
```

### Audit

```bash
./immudb-play audit kv pgaudit 100
```

Note: audit is done using primary field value which is unique, in case of pgaudit it is statement_id value.