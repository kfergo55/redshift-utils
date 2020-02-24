drop procedure acme_admin.acme_cdm_scd_batch(tablename IN varchar, where_batch IN varchar, where_date IN varchar, dw_scd_run_id_val varchar, return_result varchar);

CREATE OR REPLACE PROCEDURE acme_admin.acme_cdm_scd_batch(tablename IN varchar(50), where_batch varchar(200), where_date varchar(30), dw_scd_run_id_val varchar(50), return_result OUT varchar(100))
AS $$
  DECLARE
col_rec RECORD;
meta_rec RECORD;
query varchar(10000);
hist_tablename varchar(120);

column_length integer;
BEGIN
    /* batch_id (list) IN
    1. --get scd_type, key_name and source table from metadata
    2. --get columns into column_string, column_string_noedate for this table from metadata
    */
   
    query := 'SELECT scd_type, sdm_table_name, primary_key from acme_admin.acme_cdm1_map_lv1 where upper(schema||''.''||table_name) = '||quote_literal(upper(tablename));
    FOR meta_rec IN EXECUTE query
        LOOP
        RAISE INFO 'meta: % : % : %', meta_rec.sdm_table_name, meta_rec.scd_type, meta_rec.primary_key;
        END LOOP;
    query := 'SELECT listagg(col_name, '', '') within group (order by table_name) as column_string, regexp_replace(column_string,''dw_date_partitioned'','' '') as column_string_noedate from acme_admin.acme_cdm1_map_lv2 where upper(table_name) = '||quote_literal(upper(tablename));
    FOR col_rec IN EXECUTE query
        LOOP
        column_length := len(col_rec.column_string);
        RAISE INFO 'col: % : % : %', column_length, col_rec.column_string, col_rec.column_string_noedate;
        END LOOP;
    return_result := 'processing';

/*
  --Different logic based on the SCD type -
*/
  
  /*
  --SCD1 LOGIC BLOCK -
  */
    IF meta_rec.scd_type = 'scd1' THEN
        RAISE INFO 'in scd1 logic';

        CALL acme_admin.acme_scd1_logic(tablename, where_batch, where_date, meta_rec.sdm_table_name, meta_rec.primary_key,
                              col_rec.column_string, dw_scd_run_id_val, return_result);
        RAISE INFO '%',return_result;

    RETURN;
    END IF;

/*
  --SCD2 LOGIC BLOCK
*/
    IF meta_rec.scd_type = 'scd2' THEN
        RAISE INFO 'scd2: ';

        CALL acme_admin.acme_scd2_logic(tablename, where_batch, where_date, meta_rec.sdm_table_name, meta_rec.primary_key,
                              col_rec.column_string, return_result);
        RAISE INFO '%',return_result;
    RETURN;
END IF;
/*
  SCD4 LOGIC BLOCK
*/
    IF meta_rec.scd_type = 'scd4' THEN
        RAISE INFO 'scd4: ';
        hist_tablename := tablename||'_hist';

        CALL acme_admin.acme_scd1_logic(tablename, where_batch, where_date, meta_rec.sdm_table_name, meta_rec.primary_key,
                              col_rec.column_string, return_result);
        RAISE INFO '%',return_result;

        CALL acme_admin.acme_scd2_logic(hist_tablename, where_batch, where_date, meta_rec.sdm_table_name, meta_rec.primary_key,
                              col_rec.column_string, return_result);
        RAISE INFO '%',return_result;

    RETURN;
END IF;
END;
$$ LANGUAGE plpgsql;

/* sample call
call acme_admin.acme_cdm_scd_batch('acme_cdm_schema.acme_cdm_SBL_CONTACT','dw_batchid=''siebel-ingress-001-1577434429759''', quote_literal('2020-01-08'), 'hxx-scd-1577434429759');
*/
  
drop procedure acme_admin.acme_scd1_logic(tablename IN varchar, where_batch IN varchar, where_date IN varchar,
                                          sdm_table_name IN varchar, primary_key IN varchar, column_string IN varchar, dw_scd_run_id_val IN varchar, return_result INOUT int);

CREATE OR REPLACE PROCEDURE acme_admin.acme_scd1_logic(tablename IN varchar(120), where_batch IN varchar(200),
                                                     where_date IN varchar(50), sdm_table_name IN varchar(120), primary_key IN varchar(50), column_string IN varchar(7000),
                                                     dw_scd_run_id_val IN varchar(50), return_result INOUT varchar(120))
AS $$
  DECLARE
query varchar(10000);
execute_return int;
meta_rec RECORD;
BEGIN
return_result := ': ';

RAISE INFO 'primarykeyname: %', primary_key;

/* 1 -- hard delete where active_flag = 'N' */
                              
/*
    query := 'delete from '||tablename||' where '||tablename||'.'||primary_key||' in (select '||
            sdm_table_name||'.'||primary_key||' from '||sdm_table_name||' where '||tablename||
            '.dw_date_partitioned = '||where_date||' AND '||where_batch||' AND dw_active_flag = ''N'')' ;
    EXECUTE query;
*/
                              
    query := 'select count(*) AS COUNT_SDM from '||sdm_table_name||' where dw_date_partitioned = '||where_date||' AND '||where_batch;
    FOR meta_rec IN EXECUTE query LOOP END LOOP;
    RAISE INFO 'row count: %', meta_rec.COUNT_SDM;
    return_result := return_result||meta_rec.COUNT_SDM;
                            
    /* 2.1 -- hard delete by key and date_partitioned != new_date_partitioned --
            only one record per primary key so just delete...
     */
                              
    query := 'delete from '||tablename||' where '||primary_key||' in (select '||primary_key||' from '||
        sdm_table_name||' where dw_date_partitioned = '||where_date||' AND '||where_batch||')';
    EXECUTE query;
    GET DIAGNOSTICS execute_return := ROW_COUNT;
    RAISE INFO 'rows affected by execute: %', execute_return;
    return_result := return_result||', '||execute_return;
                            
    /* 2.2 -- upsert will be a insert of all + delete on key match */
                              
    query := 'insert into '||tablename||' ('||column_string||',dw_scd_run_id) select '||column_string||','||quote_literal(dw_scd_run_id_val)||
        ' from '||sdm_table_name||' where dw_date_partitioned = '||where_date||' AND '||where_batch;
    EXECUTE query;
    GET DIAGNOSTICS execute_return := ROW_COUNT;
    RAISE INFO 'rows affected by execute: %', execute_return;
    return_result := return_result||', '||execute_return;
                            
    /* return count of records - this check confirms no dups */
    query := 'select count(*) AS COUNT_CDM from '||tablename||' where dw_scd_run_id = '||quote_literal(dw_scd_run_id_val);
    FOR meta_rec IN EXECUTE query LOOP END LOOP;
    RAISE INFO 'row count: %', meta_rec.COUNT_CDM;
    return_result := 'SUCCESS'||return_result||', '||meta_rec.COUNT_CDM;
                            
END;
$$ LANGUAGE plpgsql;
