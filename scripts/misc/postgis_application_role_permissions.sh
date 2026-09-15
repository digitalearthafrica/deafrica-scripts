#!/usr/bin/env bash

# Align existing ODC, OWS, sandbox and Explorer accounts with ODC 1.9 PostGIS.
# This script grants roles only: it does not create database users or secrets.
#
# From an EKS shell, export the existing DBA credentials in one command:
# export DB_USERNAME="$(kubectl -n admin get secret dba-admin -o jsonpath='{.data.postgres-username}' | base64 -d)" DB_PASSWORD="$(kubectl -n admin get secret dba-admin -o jsonpath='{.data.postgres-password}' | base64 -d)" DB_DATABASE=odc_v19 DB_HOSTNAME=<writer-host>
#
# Use DB_HOSTNAME=db-writer only when running inside the cluster. Override role
# variables below only where an environment uses names other than the defaults.
set -euo pipefail

DB_DATABASE="${DB_DATABASE:-odc_v19}"
DB_HOSTNAME="${DB_HOSTNAME:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_ADMIN_USER="${DB_USERNAME:-dba_admin}"
DB_ADMIN_PASSWORD="${DB_PASSWORD:-}"
ODC_SCHEMA="${ODC_SCHEMA:-odc}"
EXPLORER_SCHEMA="${EXPLORER_SCHEMA:-cubedash}"
ODC_WRITER_USER="${ODC_WRITER_USER:-odc_writer}"
OWS_WRITER_USER="${OWS_WRITER_USER:-ows_writer}"
SANDBOX_READER_USER="${SANDBOX_READER_USER:-sandbox_reader}"
EXPLORER_READER_USER="${EXPLORER_READER_USER:-explorer_reader}"
EXPLORER_WRITER_USER="${EXPLORER_WRITER_USER:-explorer_writer}"
EXPLORER_ADMIN_USER="${EXPLORER_ADMIN_USER:-explorer_admin}"

export PGPASSWORD="${DB_ADMIN_PASSWORD}"
psql_args=(-h "${DB_HOSTNAME}" -p "${DB_PORT}" -U "${DB_ADMIN_USER}" -d "${DB_DATABASE}")

psql "${psql_args[@]}" -v ON_ERROR_STOP=1 \
  -v odc_schema="${ODC_SCHEMA}" \
  -v explorer_schema="${EXPLORER_SCHEMA}" <<'SQL'
SELECT set_config('dea.odc_schema', :'odc_schema', false);
SELECT set_config('dea.explorer_schema', :'explorer_schema', false);
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = current_setting('dea.odc_schema')) THEN
    RAISE EXCEPTION 'ODC schema % does not exist', current_setting('dea.odc_schema');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = current_setting('dea.explorer_schema')) THEN
    RAISE EXCEPTION 'Explorer schema % does not exist', current_setting('dea.explorer_schema');
  END IF;
END $$;
SQL

for role in odc_user odc_manage odc_admin "${ODC_WRITER_USER}" "${OWS_WRITER_USER}" "${SANDBOX_READER_USER}" "${EXPLORER_READER_USER}" "${EXPLORER_WRITER_USER}" "${EXPLORER_ADMIN_USER}"; do
  if ! psql "${psql_args[@]}" -Atqc "SELECT 1 FROM pg_roles WHERE rolname = '${role}'" | grep -q 1; then
    echo "Missing required role: ${role}" >&2
    exit 1
  fi
done

psql "${psql_args[@]}" -v ON_ERROR_STOP=1 \
  -v odc_schema="${ODC_SCHEMA}" -v explorer_schema="${EXPLORER_SCHEMA}" \
  -v odc_writer_user="${ODC_WRITER_USER}" -v ows_writer_user="${OWS_WRITER_USER}" \
  -v sandbox_reader_user="${SANDBOX_READER_USER}" \
  -v explorer_reader_user="${EXPLORER_READER_USER}" \
  -v explorer_writer_user="${EXPLORER_WRITER_USER}" \
  -v explorer_admin_user="${EXPLORER_ADMIN_USER}" <<'SQL'
SELECT set_config('dea.odc_schema', :'odc_schema', false);
SELECT set_config('dea.explorer_schema', :'explorer_schema', false);
SELECT set_config('dea.odc_writer_user', :'odc_writer_user', false);
SELECT set_config('dea.ows_writer_user', :'ows_writer_user', false);
SELECT set_config('dea.sandbox_reader_user', :'sandbox_reader_user', false);
SELECT set_config('dea.explorer_reader_user', :'explorer_reader_user', false);
SELECT set_config('dea.explorer_writer_user', :'explorer_writer_user', false);
SELECT set_config('dea.explorer_admin_user', :'explorer_admin_user', false);
DO $$
DECLARE
  odc_schema text := current_setting('dea.odc_schema');
  explorer_schema text := current_setting('dea.explorer_schema');
  odc_writer_user text := current_setting('dea.odc_writer_user');
  ows_writer_user text := current_setting('dea.ows_writer_user');
  sandbox_reader_user text := current_setting('dea.sandbox_reader_user');
  explorer_reader_user text := current_setting('dea.explorer_reader_user');
  explorer_writer_user text := current_setting('dea.explorer_writer_user');
  explorer_admin_user text := current_setting('dea.explorer_admin_user');
BEGIN
  EXECUTE format('GRANT odc_user TO %I', odc_writer_user);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', odc_schema, odc_writer_user);
  EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO %I', odc_schema, odc_writer_user);
  EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I', odc_schema, odc_writer_user);
  EXECUTE format('GRANT odc_manage TO %I', ows_writer_user);
  EXECUTE format('GRANT odc_user TO %I', sandbox_reader_user);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', odc_schema, sandbox_reader_user);
  EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', odc_schema, sandbox_reader_user);
  EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I', odc_schema, sandbox_reader_user);
  EXECUTE format('GRANT odc_user TO %I', explorer_reader_user);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', explorer_schema, explorer_reader_user);
  EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', explorer_schema, explorer_reader_user);
  EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I', explorer_schema, explorer_reader_user);
  EXECUTE format('GRANT odc_manage TO %I', explorer_writer_user);
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO %I', explorer_schema, explorer_writer_user);
  EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO %I', explorer_schema, explorer_writer_user);
  EXECUTE format('GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA %I TO %I', explorer_schema, explorer_writer_user);
  EXECUTE format('GRANT odc_admin TO %I', explorer_admin_user);
  EXECUTE format('ALTER SCHEMA %I OWNER TO %I', explorer_schema, explorer_admin_user);
END $$;
SQL

echo "PostGIS application role alignment complete for ${DB_DATABASE}"
