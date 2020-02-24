# redshift-utils

## Source data model to Conformed data model  (source-to-conformed-batch)
This utility copies data from a "source data model" to a "conformed data model" as defined by slowly changing dimensions. SCD1 is an overwrite, SCD2 is an append, SCD4 is an overwrite on a base table + an append on a base_hist table.
The source data model resides as parquet files in S3. Each table has a directory structure partitioned by the date and a unique batchid.
The conformed data model resides as Redshift internal tables.

source-to-conformed-batch.sql - contains the DDL to build the database objects in Redshift 

> To execute: for N tables per batch:
* for Siebel table S_CONTACT
+ call acme_admin.acme_cdm_scd_batch('acme_cdm_schema.acme_cdm_SBL_CONTACT','dw_batchid=''siebel-ingress-001-1577434429759''', quote_literal('2020-01-08'), 'hxx-scd-1577434429759');
* for Siebel table S_PARTY
+ call acme_admin.acme_cdm_scd_batch('acme_cdm_schema.acme_cdm_SBL_PARTY','dw_batchid=''siebel-ingress-001-1577434429759''', quote_literal('2020-01-08'), 'hxx-scd-1577434429759');
