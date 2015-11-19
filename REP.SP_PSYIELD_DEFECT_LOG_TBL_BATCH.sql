CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_DEFECT_LOG_TBL_BATCH()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	DECLARE 
		pExistFlag boolean;
		pTableName varchar;
		pTableSchema varchar;
		pBatchDefectLogView varchar;
	
	BEGIN
		
		pTableName := 'PSYIELD_DEFECT_LOG_DENORM_TMP';
		pTableSchema := 'REP';
		
		pBatchDefectLogView := '
								SELECT * FROM (
												SELECT 
													HIS_LOT_DEFECT.DEFECT_CODE, 
													HIS_LOT_DEFECT.DEFECT_QUANTITY, 
													H.SITE_KEY,
													H.SITE_CODE, 
													H.LOT_KEY,
													H.LOT_ID, 
													H.IS_TEST_LOT_ID_FLAG, 
													H.M_LH_OPERATION_KEY, 
													H.M_LH_OPERATION_NAME, 
													H.M_LH_PROGRAM_KEY, 
													H.M_LH_PROGRAM_NAME,
													H.M_LH_PRODUCT_KEY, 
													H.M_LH_PRODUCT_NAME,
													H.M_LH_FACILITY_KEY, 
													H.M_LH_FACILITY,
													H.M_LH_OPERATION_DATE, 
													H.M_LH_OPERATION_YEAR, 
													H.M_LH_OPERATION_QUARTER, 
													H.M_LH_OPERATION_WEEK, 
													H.M_LH_OWNER, 
													H.M_LH_QUANTITY_ONHAND,  
													H.M_LH_TRANSACTION_NAME, 
													H.M_LH_DELETED_FLAG, 
													H.M_LH_GROUP_FLAG, 
													H.M_LH_OLD_QUANTITY_ONHAND, 
													H.M_LH_OLD_ROUTE_KEY, 
													H.M_LH_OLD_ROUTE_NAME, 
													H.M_LH_OLD_ROUTE_NAME_REWORK_SUFFIX, 
													H.M_LH_OLD_PRODUCT_KEY, 
													H.M_LH_OLD_PRODUCT_NAME, 
													H.M_LH_OLD_PROGRAM_KEY, 
													H.M_LH_OLD_PROGRAM_NAME, 
													H.M_LH_OLD_PROGRAM_WITH_PN_SUFFIX_FLAG, 
													H.M_LH_OLD_FACILITY_KEY, 
													H.M_LH_OLD_FACILITY, 
													H.M_LH_OLD_OPERATION_KEY, 
													H.M_LH_OLD_OPERATION_NAME,
													ROW_NUMBER() OVER ( PARTITION BY HIS_LOT_DEFECT.LOT_KEY, HIS_LOT_DEFECT.SITE_KEY, HIS_LOT_DEFECT.DEFECT_KEY ORDER BY LOT_HISTORY_SEQUENCE DESC ) AS RowNum
												FROM 
													ADS.VIEW_HIS_LOT_DEFECT HIS_LOT_DEFECT 
													LEFT JOIN REP.PSYIELD_LOT_HIS_DENORM H ON ( (HIS_LOT_DEFECT.SITE_KEY = H.SITE_KEY) AND (HIS_LOT_DEFECT.LOT_KEY = H.LOT_KEY) )
											) TMP WHERE  TMP.RowNum = 1
		
								';
		
		--	RAISE NOTICE 'pBatchDefectLogView=%', pBatchDefectLogView;
			
		SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pExistFlag FROM _V_TABLE WHERE TABLENAME = pTableName AND SCHEMA = pTableSchema;
		
		IF pExistFlag  = TRUE THEN
			EXECUTE IMMEDIATE 'DROP TABLE ' || pTableSchema || '.'||pTableName;
			RAISE NOTICE 'Table %.% exist, deleted.', pTableSchema, pTableName;
		END IF;
	
		EXECUTE IMMEDIATE 'CREATE TABLE '|| pTableSchema || '.'||pTableName ||' AS SELECT * FROM ( ' || pBatchDefectLogView || ' ) t DISTRIBUTE ON (SITE_CODE , M_LH_OLD_PROGRAM_NAME)';
		RAISE NOTICE 'create Table %.%, .', pTableSchema, pTableName;
			
	END;
END_PROC;

