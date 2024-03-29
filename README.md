# immudb-audit

immudb-audit is a simple service and cli tool to store json formatted log input and audit it later in immudb key-value or SQL.

Example input log source

```json
{"field1":1, "field2":"abc", "field3": "2023-03-10T22:38:26.461908Z", "group": {"field4":"cde"}}
{"field1":2, "field2":"cde", "field3": "2023-03-10T22:38:26.461908Z", "group": {"field4":"cde"}}
{"field1":3, "field2":"efg", "field3": "2023-04-10T22:38:26.461908Z", "group": {"field4":"cde"}}
{"field1":4, "field2":"ijk", "field3": "2023-05-10T22:38:26.461908Z", "group": {"field4":"cde"}}
```

In addition, immudb-audit provides two predefined log line parsers:
- pgaudit, which transforms pgaudit audit logs into json representation and stores them in immudb. 
- wrap, which accepts any log line and wraps it into json adding uid and timestamp. 


## Overview
immudb-audit uses either immudb key-value or SQL to store the data. In general, it transforms selected fields from JSON into key-values or SQL entries enabling easy and automated storage with later retrieval and audit of data. 

### Storing data
To start storing data, you need to first create a collection and define fields from source JSON which will be considered as unique primary key and indexed, or use one of available line parsers that have them predefined.

To create a custom key value collection. The indexes flag is a string slice, where the first entry is considered as primary key. Primary key can combine multiple fields from JSON, in a form field1+field2+... .

```bash
./immudb-play create kv mycollection --indexes "field1+field2,field2,field3"
```

Similarly, SQL collection can be created. The main difference is that in this case the field types need to be provided. 

```bash
./immudb-play create sql mycollection --columns "field1=INTEGER,field2=VARCHAR[256],field3=BLOB" --primary-key "field1,field2"
```

After creating a collection, data can be easily pushed using tail subcommand. immudb-play will retrieve collection definition, so there is no difference if key-value or sql was used. Currently supported sources are file and docker container. Both can be used with --follow option, which in case of files will also handle rotation.

```bash
./immudb-play tail file mycollection path/to/your/file --follow
```

```bash
./immudb-play tail docker mycollection container_name --follow --stdout --stderr
```

Note: adding --log-level trace will print what lines have been parsed and stored

The full JSON entry is always stored next to indexed fields for both key value and SQL. 

### Reading data
Reading data is more specific depending if key-value or SQL was used when creating a collection. 

For key-value, the indexed key and its value prefix can be specified to narrow down the result. Keys cannot be combined for reads. All of the values for queries are stored as string represenation of data. To read whole collection, do not specify anything.

```bash
./immudb-play read kv mycollection
./immudb-play read kv mycollection field=abc
```

For SQL, read command will accept the condition as for SQL statement after WHERE clause. If not specified, all rows are returned
```bash
./immudb-play read sql mycollection 
./immudb-play read sql mycollection "field LIKE '(99.)'"
```

### Auditing data
Auditing data is more specific depending if key-value or SQL was used when creating a collection.

For key-vale, the audit accepts exact value of primary key, and returns information about TXID, Revision and Value entry itself.

```bash
./immudb-play audit kv mycollection primarykeyvalue
```

For SQL, the audit accepts temporal query statement and returns matching values at a given time.
```
./immudb-play audit sql mycollection
./immudb-play audit sql mycollection "SINCE TX 2000"
```

## Storing pgaudit logs in immudb
[pgaudit](https://github.com/pgaudit/pgaudit) is PostgreSQL extension that enables audit logs for the database. Any kind of audit logs should be stored in secure location. immudb is fullfiling this requirement with its immutable and tamper proof features.

immudb-audit has simple pgaudit log line parser. It assumes that each log line has log_line_prefix of '%m [%p] '.

To start, you need to have an PostgreSQL running with pgaudit extension enabled. As the example, [bitnami postgresql](https://hub.docker.com/r/bitnami/postgresql) which already hase pgaudi extension can be used. 

The example pgaudi log line looks like:
```
2023-02-03 21:15:01.851 GMT [294] LOG:  AUDIT: SESSION,61,1,WRITE,INSERT,,,"insert into audit_trail(id, ts, usr, action, sourceip, context) VALUES ('134ff2d5-2db4-44d2-9f67-9c7f5ed64967', NOW(), 'user60', 1, '127.0.0.1', 'some context')",<not logged>
```

pgaudit parser will convert each log line into following json
```json
{"timestamp":"2023-03-16T08:58:44.033611299Z","log_timestamp":"2023-03-02T21:15:01.851Z","audit_type":"SESSION","statement_id":61,"substatement_id":1,"class":"WRITE","command":"INSERT","statement":"insert into audit_trail(id, ts, usr, action, sourceip, context) VALUES ('134ff2d5-2db4-44d2-9f67-9c7f5ed64967', NOW(), 'user60', 1, '127.0.0.1', 'some context')","parameter":"\u003cnot logged\u003e"}
```

The indexed fields for pgaudit are
```
statement_id log_timestamp timestamp audit_type class command
```

### How to set up

You can use [docker-compose.yml](test/pgaudit/docker-compose.yml) from this repository as an example.

```bash
docker-compose up -d -f test/pgaudit/docker-compose.yml
```

Note: you can execute ```go run test/utils/psql.go``` to generate audit entries.

Create immudb-audit collection for logs

```bash
./immudb-play create kv pgaudit --parser pgaudit
```

Tail PostgreSQL docker container logs

```bash
./immudb-play tail docker pgaudit psql-postgresql-1 --stdout --stderr --follow
```

Optionally, adding --log-level trace will print out all lines parsed and stored. 

Read

```bash
./immudb-play read kv pgaudit statement_id=100
./immudb-play read kv pgaudit command=INSERT
```

Audit

```bash
./immudb-play audit kv pgaudit 100
```

Note: audit is done using primary field value which is unique, in case of pgaudit it is statement_id value.

## Storing kubernetes audit logs in immudb
Kubernetes allow audit logging showing the track of actions taken in the cluster. To enable kubernets audit, follow the [documentation](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/).

Kubernetes audit logs are stored as Json structured log, so can be easily parsed and stored in immudb with immudb-audit. For this exercise, you can use example [k8s.log](test/k8s/k8s.log) from this repository.

Sample json log line

```json
{"kind":"Event","apiVersion":"audit.k8s.io/v1","level":"Metadata","auditID":"d4652481-193e-42f6-9b78-f8651cab5dfe","stage":"RequestReceived","requestURI":"/api?timeout=32s","verb":"get","user":{"username":"admin","uid":"admin","groups":["system:masters","system:authenticated"]},"sourceIPs":["127.0.0.1"],"userAgent":"kubectl/v1.25.6 (linux/amd64) kubernetes/ff2c119","requestReceivedTimestamp":"2023-03-10T22:38:26.382098Z","stageTimestamp":"2023-03-10T22:38:26.382098Z"}
```

### How to set up

First, create k8saudit collection, with primary key as auditID and stage, and addtional indexes as kind, stage and user.username. More indexes can be added if needed.

```bash
./immudb-play create kv k8s --indexes auditID+stage,kind,stage,user.username
```

Tail k8s.log 

```bash
./immudb-play tail file k8s test/k8s/k8s.log
```

Read

```bash
./immudb-play read kv k8s
./immudb-play read kv k8s user.username=admin
./immudb-play read kv k8s stage=ResponseStarted

```

Audit

```bash
./immudb-play audit kv k8s d4652481-193e-42f6-9b78-f8651cab5dfeRequestReceived
```
Note: as primary key is auditID+stage, in audit their values need to be concatenated. 

## Storing unstructured logs in immudb
immudb-audit provides "wrap" parser, which wraps any log line with autogenerated uid and timestamp. In example, given following syslog line:

```
Jan  6 13:57:19 DESKTOP-BLRRBQO kernel: [    0.000000] Hyper-V: privilege flags low 0xae7f, high 0x3b8030, hints 0xc2c, misc 0xe0bed7b2
```

It will convert it to:
```json
{"uid":"6326acda-e254-481f-b030-0144141df091","log_timestamp":"2023-03-16T10:23:25.554276817+01:00","message":"Jan  6 13:57:19 DESKTOP-BLRRBQO kernel: [    0.000000] Hyper-V: privilege flags low 0xae7f, high 0x3b8030, hints 0xc2c, misc 0xe0bed7b2"}
```

### How to set up
[Syslog file](test/syslog/syslog) is used as an example, but source can be any file our log output from a docker container.

First, create syslog collection with "wrap" parser.

```bash
./immudb-play create sql syslog --parser wrap
```

Tail syslog 

```bash
./immudb-play tail file syslog test/syslog/syslog
```

Read

```bash
 ./immudb-play read sql syslog
 ./immudb-play read sql syslog "log_timestamp > CAST('2023-03-16 9:36:58.49' as TIMESTAMP)"
```

Audit

```bash
./immudb-play audit sql syslog
```
