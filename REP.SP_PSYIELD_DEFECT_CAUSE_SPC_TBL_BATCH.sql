CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_DEFECT_CAUSE_SPC_TBL_BATCH()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	
	DECLARE 
		pExistFlag boolean;
		pTableName varchar;
		pTableSchema varchar;
		pBatchDefectSpcView varchar;

BEGIN
	BEGIN AUTOCOMMIT ON
		pTableName := 'PSYIELD_DEFECT_CAUSE_SPC_DENORM_TMP';
		pTableSchema := 'REP';
		pBatchDefectSpcView := '
								SELECT 
									OST.OPERATION_DESCRIPTION,
									WS.SITE_CODE AS M_LH_SITE_CODE,
									WS.M_LASL_WAFER_SECTION_ID,
									WS.M_LH_OLD_OPERATION_NAME,
									WS.M_LH_OPERATION_DATE,
									WS.M_LH_OPERATION_YEAR,
									WS.M_LH_OPERATION_QUARTER,
									WS.M_LH_OPERATION_WEEK,
									WS.PROGRAM,
									WS.DEFECT_CODE,
									WS.M_DL_DEFECT_QTY,
									MES.PROCESS_TOOL_NAME AS SPC_M_PROCESS_TOOL,
									MES.TEST_DATE_TIME AS SPC_M_TEST_DATE_TIME,
									RECIPE.TAG_VALUE AS SPC_RECIPE_VALUE,
									RUNNO.TAG_VALUE AS SPC_RUNNO_VALUE,
									START_TIME.TAG_VALUE AS SPC_START_TIME_VALUE,
									END_TIME.TAG_VALUE AS SPC_END_TIME_VALUE
								FROM 
								(
									SELECT 	DD.SITE_KEY, DD.SITE_CODE, DD.M_LASL_WAFER_SECTION_ID, DD.M_LH_OLD_OPERATION_NAME, DD.M_LH_OPERATION_DATE, DD.M_LH_OPERATION_YEAR,  
											DD.M_LH_OPERATION_QUARTER, DD.M_LH_OPERATION_WEEK, DD.PROGRAM, DD.DEFECT_CODE, DD.M_LH_LOT_ID,
											SUM(DD.M_DL_DEFECT_QTY) AS M_DL_DEFECT_QTY
									FROM REP.PSYIELD_DEFECT_DETAIL_DENORM DD
									WHERE (LENGTH(DD.M_LASL_WAFER_SECTION_ID) > 0)
											AND (DD.M_LH_OLD_OPERATION_NAME, DD.SITE_KEY) = 
																				ANY (
																						SELECT ST.OPERATION_NAME, ST.SITE_KEY
																						FROM REP.PSYIELD_OPERATION_SETUP ST
																						WHERE ST.SUBJECT_AREA = ''SPC''::"VARCHAR"
																					)
									GROUP BY DD.SITE_KEY,DD.SITE_CODE, DD.M_LASL_WAFER_SECTION_ID,DD.M_LH_OLD_OPERATION_NAME, DD.M_LH_OPERATION_DATE, DD.M_LH_OPERATION_YEAR,
											DD.M_LH_OPERATION_QUARTER, DD.M_LH_OPERATION_WEEK, DD.PROGRAM, DD.DEFECT_CODE, DD.M_LH_LOT_ID
								) WS 
								JOIN 
									REP.PSYIELD_OPERATION_SETUP OST ON (WS.SITE_KEY = OST.SITE_KEY)
																AND (WS.M_LH_OLD_OPERATION_NAME = OST.OPERATION_NAME) 
																AND (OST.SUBJECT_AREA = ''SPC''::"VARCHAR")
								JOIN 
									( 
										SELECT DISTINCT LOT_KEY, TEST_DATE_TIME, TAG_KEY, LOT_ID,TAG_NAME, TAG_VALUE, SITE_KEY 
										FROM ADS.VIEW_PARAM_SPC_TAG
									)TAG ON (TAG.TAG_NAME ~~ LIKE_ESCAPE(''lot_number%''::"VARCHAR", ''\\''::"VARCHAR"))
																AND (TAG.SITE_KEY = WS.SITE_KEY)
																AND (TAG.TAG_NAME = WS.M_LASL_WAFER_SECTION_ID)
																AND (TAG.LOT_ID = WS.M_LH_LOT_ID)
								
								JOIN ADS.VIEW_PARAM_SPC_HEADER MES ON (MES.SITE_KEY = WS.SITE_KEY)
																AND (MES.LOT_ID = WS.M_LH_LOT_ID)
																AND (MES.TEST_DATE_TIME = TAG.TEST_DATE_TIME)
																AND (MES.LOT_KEY = TAG.LOT_KEY)
																AND (MES.OPERATION_NAME = WS.M_LH_OLD_OPERATION_NAME)
																AND (MES.PROCESS_TOOL_KEY = MES.MEASUREMENT_TOOL_KEY)
								JOIN ADS.VIEW_PARAM_SPC_TAG RECIPE ON (RECIPE.SITE_KEY = WS.SITE_KEY)
																AND (RECIPE.LOT_ID = WS.M_LH_LOT_ID)
																AND (RECIPE.TEST_DATE_TIME = TAG.TEST_DATE_TIME)
																AND (RECIPE.TAG_NAME = OST.TAG_NAME_RECIPE)
								JOIN ADS.VIEW_PARAM_SPC_TAG RUNNO ON (RUNNO.SITE_KEY = WS.SITE_KEY)
																AND (RUNNO.LOT_ID = WS.M_LH_LOT_ID)
																AND (RUNNO.TEST_DATE_TIME = TAG.TEST_DATE_TIME)
																AND (RUNNO.TAG_NAME = OST.TAG_NAME_RUN_NUMBER)
								JOIN ADS.VIEW_PARAM_SPC_TAG START_TIME ON (START_TIME.SITE_KEY = WS.SITE_KEY)
																AND (START_TIME.LOT_ID = WS.M_LH_LOT_ID)
																AND (START_TIME.TEST_DATE_TIME = TAG.TEST_DATE_TIME)
																AND (START_TIME.TAG_NAME = OST.TAG_NAME_START_TIME)
								JOIN ADS.VIEW_PARAM_SPC_TAG END_TIME ON (END_TIME.SITE_KEY = WS.SITE_KEY)
																AND (END_TIME.LOT_ID = WS.M_LH_LOT_ID)
																AND (END_TIME.TEST_DATE_TIME = TAG.TEST_DATE_TIME)
																AND (END_TIME.TAG_NAME = OST.TAG_NAME_END_TIME)
								WHERE STRPOS( MES.PROCESS_TOOL_NAME,OST.PROCESS_TOOL_PREFIX)  =1 

			';
			
		--	RAISE NOTICE 'pBatchDefectSpcView=%', pBatchDefectSpcView;
			
		SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pExistFlag FROM _V_TABLE WHERE TABLENAME = pTableName AND SCHEMA = pTableSchema;
		
		IF pExistFlag  = TRUE THEN
			EXECUTE IMMEDIATE 'DROP TABLE ' || pTableSchema || '.'||pTableName;
			RAISE NOTICE 'Table %.% exist, deleted.', pTableSchema, pTableName;
		END IF;
		
		EXECUTE IMMEDIATE 'CREATE TABLE '|| pTableSchema || '.'||pTableName ||' AS SELECT * FROM ( ' || pBatchDefectSpcView || ' ) t DISTRIBUTE ON (M_LH_SITE_CODE , PROGRAM)';
		RAISE NOTICE 'create Table %.%', pTableSchema, pTableName;
		
	END;
END;
END_PROC;

