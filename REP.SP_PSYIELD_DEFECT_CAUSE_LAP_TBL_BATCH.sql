CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_DEFECT_CAUSE_LAP_TBL_BATCH()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	
	DECLARE 
		pExistFlag boolean;
		pTableName varchar;
		pTableSchema varchar;
		pBatchDefectLapView varchar;
		
	BEGIN
		BEGIN AUTOCOMMIT ON
		
			pTableName := 'PSYIELD_DEFECT_CAUSE_LAP_DENORM_TMP';
			pTableSchema := 'REP';
			pBatchDefectLapView := '
				SELECT 
						M.SITE_CODE, 
						M.M_LH_OLD_OPERATION_NAME, 
						M.M_LH_OPERATION_DATE, 
						M.M_LH_OPERATION_YEAR, 
						M.M_LH_OPERATION_QUARTER, 
						M.M_LH_OPERATION_WEEK, 
						M.M_LH_OLD_PROGRAM_NAME, 
						M.DEFECT_CODE, 
						M.DEFECT_QUANTITY, 
						M.LOT_ID,
						PARAM_LAP_HEADER.SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE, 
						PARAM_LAP_HEADER.LAP_FLAG AS LAP_ROUGH_FINAL_FLAG, 
						PARAM_LAP_HEADER.WAFER_BAR_NUMBER AS LAP_REAL_BAR_NO,
						--------------------------------
						PARAM_LAP_HEADER.PROCESS_TOOL_NAME AS LAP_MACHINE,
						------------------------------
						PARAM_LAP_HEADER.CELL_NUMBER AS LAP_BLKL_CELL, 
						--------------------------------------------------
						PARAM_LAP_HEADER.GOOD_SLIDER_COUNT AS LAP_HEAD_NO,
						PARAM_LAP_HEADER.PROCESS_DATE_TIME AS LAP_DATE_TIME,
						PARAM_LAP_HEADER.ROW_TOOL_NAME AS LAP_ROW_TOOL_ID, 
						PARAM_LAP_HEADER.PART_NUMBER AS LAP_PLATE_NO, 
						PARAM_LAP_HEADER.ROWBOND_TOOL_NAME AS LAP_AUTO_ROW_BOND, 
						PARAM_LAP_HEADER.ARB_PROCESS_DATE_TIME AS LAP_ARB_DATE_TIME,
						--------------------------------------------------------
						NULL AS LAP_BLKL_MC_1ST_TIME, 
						--------------------------------------------------------
						PARAM_LAP_HEADER.CELL_NUMBER AS LAP_CELL_ID 
				FROM 
				(
							SELECT 	DEFECT_LOG.SITE_CODE, 
									DEFECT_LOG.LOT_KEY, 
									DEFECT_LOG.LOT_ID, 
									DEFECT_LOG.M_LH_OLD_OPERATION_NAME, 
									---------------------------------------------------
									DEFECT_LOG.M_LH_OPERATION_DATE, 
									DEFECT_LOG.M_LH_OPERATION_YEAR , 
									DEFECT_LOG.M_LH_OPERATION_QUARTER,  
									DEFECT_LOG.M_LH_OPERATION_WEEK, 
									--------------------------------------------------------
									DEFECT_LOG.M_LH_OLD_PROGRAM_NAME, 
									DEFECT_LOG.DEFECT_CODE,  
									DEFECT_LOG.DEFECT_QUANTITY,
									
									ST.OPERATION_DESCRIPTION
							FROM 																
								REP.PSYIELD_DEFECT_LOG_DENORM DEFECT_LOG		 
								JOIN (
											SELECT 	OP.SITE_CODE, OP.SITE_KEY, OP.LOT_HISTORY_SITE_CODE, OP.OPERATION_NAME, OP.REPORT_OPERATION_NAME, 
													OP.RULE_NAME, OP.RULE_NAME_02, OP.RULE_NAME_DEFECT, OP.INCLUDE_PN_SUFFIX_FLAG,
													CASE 
														WHEN (OP.YIELD_GROUP_ID = ''2A''::"VARCHAR") THEN OP.PROCESS_GROUP_ID ELSE OP.YIELD_GROUP_ID 
														END AS RPTG_PROCESS_GROUP_ID, 
													CASE 
														WHEN (OP.YIELD_GROUP_ID = ''2A''::"VARCHAR") THEN OP.PROCESS_GROUP ELSE OP.YIELD_GROUP 
														END AS RPTG_PROCESS_GROUP 
											FROM REP.PSYIELD_RULE_YIELD_OPERATION OP 
											WHERE (OP.YIELD_GROUP_ID IN ((''2A''::"VARCHAR")::VARCHAR(32), (''2B''::"VARCHAR")::VARCHAR(32), (''2C''::"VARCHAR")::VARCHAR(32))) AND OP.DELETED_FLAG =''N''
									) RULES ON (DEFECT_LOG.SITE_CODE = RULES.LOT_HISTORY_SITE_CODE) AND (DEFECT_LOG.M_LH_OLD_OPERATION_NAME = RULES.OPERATION_NAME)
										 
									LEFT JOIN REP.PSYIELD_RULE_YIELD_OWNER R01OWNER ON RULES.RULE_NAME = R01OWNER.RULE_NAME
												AND DEFECT_LOG.M_LH_OWNER = R01OWNER."OWNER"
												AND R01OWNER.YIELD_TYPE 
													IN ((''Prime''::"VARCHAR")::VARCHAR(32))
												AND R01OWNER.DELETED_FLAG =''N''
									 
									LEFT JOIN REP.PSYIELD_RULE_YIELD_FACILITY R01FACILITY ON RULES.RULE_NAME = R01FACILITY.RULE_NAME
											AND DEFECT_LOG.M_LH_OLD_FACILITY = R01FACILITY.FACILITY
											AND DEFECT_LOG.SITE_CODE = R01FACILITY.SITE_CODE
									 		AND R01FACILITY.DELETED_FLAG =''N''
											
									LEFT JOIN REP.PSYIELD_RULE_YIELD_OWNER R02OWNER ON RULES.RULE_NAME_02 = R02OWNER.RULE_NAME
											AND DEFECT_LOG.M_LH_OWNER = R02OWNER."OWNER"
											AND R02OWNER.YIELD_TYPE 
												IN ((''Prime''::"VARCHAR")::VARCHAR(32))
											AND R02OWNER.DELETED_FLAG =''N''
									 
									LEFT JOIN REP.PSYIELD_RULE_YIELD_FACILITY R02FACILITY ON ((((RULES.RULE_NAME_02 = R02FACILITY.RULE_NAME) 
											AND (DEFECT_LOG.M_LH_OLD_FACILITY = R02FACILITY.FACILITY)) 
											AND (DEFECT_LOG.SITE_CODE = R02FACILITY.SITE_CODE)))
											AND R02FACILITY.DELETED_FLAG =''N''
								
									JOIN REP.REP_OPERATION_SETUP ST ON (ST.OPERATION_NAME = DEFECT_LOG.M_LH_OLD_OPERATION_NAME) 
											AND (ST.SITE_CODE = DEFECT_LOG.SITE_CODE)
											AND (ST.SUBJECT_AREA  = ''LAPPING''::"VARCHAR") 
											AND (ST.DELETED_FLAG = ''N'')
								 
								WHERE (
											(
												((RULES.RULE_NAME, DEFECT_LOG.M_LH_OWNER ) = 
																						ANY (
																								SELECT DISTINCT ROWNER.RULE_NAME, ROWNER."OWNER" 
																								FROM REP.PSYIELD_RULE_YIELD_OWNER ROWNER
																								WHERE (ROWNER.YIELD_TYPE 
																								IN ((''Prime''::"VARCHAR")::VARCHAR(32))) 
																								AND ROWNER.DELETED_FLAG =''N''
																							)
												) 
												AND ((RULES.RULE_NAME, DEFECT_LOG.SITE_CODE, DEFECT_LOG.M_LH_OLD_FACILITY) = 
																						ANY (
																								SELECT DISTINCT RFACILITY.RULE_NAME, 
																								RFACILITY.SITE_CODE, 
																								RFACILITY.FACILITY 
																								FROM REP.PSYIELD_RULE_YIELD_FACILITY RFACILITY
																								WHERE  RFACILITY.DELETED_FLAG =''N''
																							)
													)
											) 
											OR (
													((RULES.RULE_NAME_02, DEFECT_LOG.M_LH_OWNER) = 
																						ANY (
																								SELECT DISTINCT ROWNER.RULE_NAME, ROWNER."OWNER" 
																								FROM REP.PSYIELD_RULE_YIELD_OWNER ROWNER
																								WHERE (ROWNER.YIELD_TYPE 
																								IN ((''Prime''::"VARCHAR")::VARCHAR(32)))
																								AND ROWNER.DELETED_FLAG =''N''
																							)
													) 
													AND ((RULES.RULE_NAME_02, DEFECT_LOG.SITE_CODE, DEFECT_LOG.M_LH_OLD_FACILITY) = 
																						ANY (
																								SELECT DISTINCT RFACILITY.RULE_NAME, 
																								RFACILITY.SITE_CODE, 
																								RFACILITY.FACILITY 
																								FROM REP.PSYIELD_RULE_YIELD_FACILITY RFACILITY
																								WHERE  RFACILITY.DELETED_FLAG =''N''
																							)
														)
												)
										
										)
							
				)M 															
				JOIN ADS.VIEW_PARAM_LAP_HEADER PARAM_LAP_HEADER ON (
													
															(
																(PARAM_LAP_HEADER.SOURCE_SYSTEM_CODE = ''DB_LAPPING''::"VARCHAR") 
																	AND (PARAM_LAP_HEADER.LAP_FLAG = ''F''::"VARCHAR")
																	AND (M.OPERATION_DESCRIPTION = ''db_Final Lap''::"VARCHAR")
															) 
															OR (
																	(PARAM_LAP_HEADER.SOURCE_SYSTEM_CODE = ''DB_LAPPING''::"VARCHAR") 
																	AND (PARAM_LAP_HEADER.LAP_FLAG = ''L''::"VARCHAR") 
																	AND (M.OPERATION_DESCRIPTION = ''db_Rough Lap''::"VARCHAR")
																) 
															OR (
																	(PARAM_LAP_HEADER.SOURCE_SYSTEM_CODE = ''ASL''::"VARCHAR") 
																	AND (PARAM_LAP_HEADER.LAP_FLAG = ''L''::"VARCHAR") 
																	AND (M.OPERATION_DESCRIPTION = ''ASL''::"VARCHAR")
																) 
															OR (	
																	(PARAM_LAP_HEADER.SOURCE_SYSTEM_CODE = ''CLBLKL''::"VARCHAR") 
																	AND (PARAM_LAP_HEADER.LAP_FLAG = ''F''::"VARCHAR") 
																	AND (M.OPERATION_DESCRIPTION = ''CLBLKL''::"VARCHAR")
																)
														
											)
									
			';
			
			--	RAISE NOTICE 'pBatchDefectDetailView=%', pBatchDefectDetailView;
			
			SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pExistFlag FROM _V_TABLE WHERE TABLENAME = pTableName AND SCHEMA = pTableSchema;
			
			IF pExistFlag  = TRUE THEN
				EXECUTE IMMEDIATE 'DROP TABLE ' || pTableSchema || '.'||pTableName;
				RAISE NOTICE 'Table %.% exist, deleted.', pTableSchema, pTableName;
			END IF;
			
			EXECUTE IMMEDIATE 'CREATE TABLE '|| pTableSchema || '.'||pTableName ||' AS SELECT * FROM ( ' || pBatchDefectLapView || ' ) t DISTRIBUTE ON (SITE_CODE , M_LH_OLD_PROGRAM_NAME)';
			RAISE NOTICE 'create Table %.%', pTableSchema, pTableName;
		
		END;

	END;
END_PROC;

