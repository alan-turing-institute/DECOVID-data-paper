# Data Definition Language
These files define the PostgreSQL version of the OMOP schema which the DECOVID project will use. All data should be inserted into this schema before submission, as many quality checks are embedded.

The accompanying bash script can be used to deploy the schema on UNIX systems.

```bash
bash build.sh -h <host> -p <port> -U <username> -W <password> -d <database> -s <schema>
```
All inputs are optional, but if not supplied then PostgreSQL defaults will be used (i.e. 127.0.0.1:5432, postgres database, public schema, user and password dependent on configuration but likely to be system username and empty string).

Additionally, two flags can be supplied:
```bash
-t
```
This specifies tables_only, so the schema tables will be created but will not have any indexes or constraints added.
```bash
-v
```
This specifies that vocabularies should be inserted into the relevant tables (before addition of indexes and constraints, if tables_only is not specified). The appropriate files must be extracted into this directory with the names unchanged. They can be downloaded from https://athena.ohdsi.org/vocabulary/list