CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_LOT_HIS_AGG_TBL_BATCH()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	DECLARE 
		pExistFlag boolean;
		pTableName varchar;
		pTableSchema varchar;
		pBatchLotHisAggView varchar;
	BEGIN
		BEGIN AUTOCOMMIT ON
		
			pTableName := 'PSYIELD_LOT_HIS_AGG_TMP';
			pTableSchema := 'REP';
			
			pBatchLotHisAggView:= 'select 
										SITE_CODE 
										,IS_TEST_LOT_ID_FLAG
										,M_LH_OWNER
										,M_LH_TRANSACTION_NAME
										,M_LH_DELETED_FLAG
										,M_LH_GROUP_FLAG
										,M_LH_OLD_ROUTE_NAME
										,M_LH_OLD_ROUTE_NAME_REWORK_SUFFIX
										,M_LH_OLD_PROGRAM_NAME
										,M_LH_OLD_PROGRAM_WITH_PN_SUFFIX_FLAG
										,M_LH_OLD_FACILITY
										,M_LH_OLD_OPERATION_NAME
										,M_LH_OPERATION_DATE
										,M_LH_OPERATION_YEAR
										,M_LH_OPERATION_QUARTER
										,M_LH_OPERATION_WEEK
										,sum(M_LH_OLD_QUANTITY_ONHAND) SUM_IN
										,sum(M_LH_QUANTITY_ONHAND) SUM_OUT
										,sum(M_LH_OLD_QUANTITY_ONHAND)-sum(M_LH_QUANTITY_ONHAND) SUM_LOSS
									from REP.PSYIELD_LOT_HIS_DENORM  
									where M_LH_DELETED_FLAG=''N'' and M_LH_GROUP_FLAG=''N''
									group by 
										SITE_CODE 
										,IS_TEST_LOT_ID_FLAG
										,M_LH_OWNER
										,M_LH_TRANSACTION_NAME
										,M_LH_DELETED_FLAG
										,M_LH_GROUP_FLAG
										,M_LH_OLD_ROUTE_NAME
										,M_LH_OLD_ROUTE_NAME_REWORK_SUFFIX
										,M_LH_OLD_PROGRAM_NAME
										,M_LH_OLD_PROGRAM_WITH_PN_SUFFIX_FLAG
										,M_LH_OLD_FACILITY
										,M_LH_OLD_OPERATION_NAME
										,M_LH_OPERATION_DATE
										,M_LH_OPERATION_YEAR
										,M_LH_OPERATION_QUARTER
										,M_LH_OPERATION_WEEK
										
										';
			
			--	RAISE NOTICE 'pBatchLotHisAggView=%', pBatchLotHisAggView;
			
			SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pExistFlag FROM _V_TABLE WHERE TABLENAME = pTableName AND SCHEMA = pTableSchema;
			
			IF pExistFlag  = TRUE THEN
				EXECUTE IMMEDIATE 'DROP TABLE ' || pTableSchema || '.'||pTableName;
				RAISE NOTICE 'Table %.% exist, deleted.', pTableSchema, pTableName;
			END IF;
			
			EXECUTE IMMEDIATE 'CREATE TABLE '|| pTableSchema || '.'||pTableName ||' AS SELECT * FROM ( ' || pBatchLotHisAggView || ' ) t DISTRIBUTE ON (SITE_CODE , M_LH_OLD_PROGRAM_NAME)';
			RAISE NOTICE 'create Table %.%, .', pTableSchema, pTableName;
			
		END;

	END;
END_PROC;

