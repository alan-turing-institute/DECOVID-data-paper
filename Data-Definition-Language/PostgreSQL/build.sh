#!/bin/bash
# bash script to deploy the DECOVID version of the OMOP schema in Postgres
# Optional addition of indexes, constraints, and contents of vocabulary tables
# Author Ruairidh King, r.king@har.mrc.ac.uk, 2020


function check_argument {
    string=$1
    argument=$2

    if [[ ${string:0:1} == '-' ]]; then
        echo "WARNING: Argument '$string' supplied for $argument looks like an option. Have you missed out the argument?"
    fi
}


tables_only="false"
vocabularies="false"

while getopts "h:p:U:W:d:s:tv" opt; do
  case $opt in
    h) host="$OPTARG"
    check_argument $host "host"
    ;;
    p) port="$OPTARG"
    check_argument $port "port"
    ;;
    U) user="$OPTARG"
    check_argument $user "user"
    ;;
    W) password="$OPTARG"
    check_argument $password "password"
    ;;
    d) database="$OPTARG"
    check_argument $database "database"
    ;;
    s) schema="$OPTARG"
    check_argument $schema "schema"
    ;;
    t) tables_only="true"
    ;;
    v) vocabularies="true"
    ;;
    \?) echo "Invalid option -$OPTARG"
    echo "Arguments: -h host -p port -U user -W password -d database -s schema -t (tables_only) -v (vocabularies)"
    exit
    ;;
    : ) echo "Option -$OPTARG requires an argument"
    exit
  esac
done

export PGPASSWORD=$password

if [[ -n $host ]]; then
  host="-h $host"tables_only
fi

if [[ -n $port ]]; then
  port="-p $port"
fi

if [[ -n $user ]]; then
  user="-U $user"
fi

if [[ -n $database ]]; then
  echo "select 'create database $database' where not exists (select from pg_database where datname = '$database')\gexec" | psql $host $port $user
  database="-d $database"
fi

project_root=$(dirname $(dirname $(dirname $(realpath $0))))

vocab_files="DRUG_STRENGTH CONCEPT CONCEPT_RELATIONSHIP CONCEPT_ANCESTOR CONCEPT_SYNONYM VOCABULARY RELATIONSHIP CONCEPT_CLASS DOMAIN"

if $vocabularies; then
  for vocab_file in $vocab_files; do
    if ! test -f "$project_root/DDLs/PostgreSQL/$vocab_file.csv"; then
      echo "-v flag was entered but $project_root/DDLs/PostgreSQL/$vocab_file vocabulary file not found"
      exit
    fi
  done
fi

tables="$project_root/DDLs/PostgreSQL/OMOP_CDM_postgresql_ddl.txt"
tables_temp="$project_root/DDLs/PostgreSQL/OMOP_CDM_postgresql_ddl_temp.txt"
touch $tables_temp

if [[ -n $schema ]]; then
  echo "create schema if not exists $schema;" >> $tables_temp
  echo "set search_path to $schema;" >> $tables_temp
fi

cat $tables >> $tables_temp
psql $host $port $user $database -f $tables_temp
rm $tables_temp

if $vocabularies; then
  touch vocabs_temp.csv
  echo "set search_path to $schema;" >> vocabs_temp.csv

  for vocab_file in $vocab_files; do
    echo "copy $vocab_file from '$project_root/DDLs/PostgreSQL/$vocab_file.csv' with delimiter e'\t' csv header quote e'\b';"  >> vocabs_temp.csv
  done

  psql $host $port $user $database -f vocabs_temp.csv
  rm vocabs_temp.csv
fi

if ! $tables_only; then
  indexes="$project_root/DDLs/PostgreSQL/OMOP_CDM_postgresql_indexes.txt"
  constraints="$project_root/DDLs/PostgreSQL/OMOP_CDM_postgresql_constraints.txt"
  additions_temp="$project_root/DDLs/PostgreSQL/additions_temp.txt"
  touch $additions_temp

  if [[ -n $schema ]]; then
    echo "set search_path to $schema;" >> $additions_temp
  fi

  cat $indexes $constraints >> $additions_temp
  psql $host $port $user $database -f $additions_temp
  rm $additions_temp
fi