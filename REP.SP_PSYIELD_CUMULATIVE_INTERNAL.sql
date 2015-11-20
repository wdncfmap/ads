CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_CUMULATIVE_INTERNAL(CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), BIGINT)
RETURNS REFTABLE(MHO_ASIA_BI_DEV.REP.SPTBL_PSYIELD_CUMULATIVE)
LANGUAGE NZPLSQL AS
BEGIN_PROC

	DECLARE
	    pSiteCode ALIAS FOR $1;
	    pYieldGroup ALIAS FOR $2;
	    pYieldType ALIAS FOR $3;
	    pProgramList ALIAS FOR $4;
	    pPeriodType ALIAS FOR $5;
	    pPeriodStart ALIAS FOR $6;
	    pPeriodEnd ALIAS FOR $7;
	    pRowNumberOffset ALIAS FOR $8;

	    pPeriodFields varchar;
	    pPeriodFieldKey varchar;
	    sqlCalendar varchar;
	    sqlYieldTarget varchar;
	    sqlRulesOperation varchar;
	    sqlDefectLog varchar;
	    sqlReworkLoss varchar;
	    sqlYieldActual varchar;
	    sqlFinal varchar;
	    noticeOutput varchar;

	BEGIN
		BEGIN AUTOCOMMIT ON
	
			noticeOutput := 'SP_PSYIELD_CUMULATIVE_INTERNAL'||chr(13) ||'Raw parameters:'||chr(13) ||'pSiteCode='||pSiteCode||chr(13) ||'pYieldGroup='||pYieldGroup||chr(13) ||'pYieldType='||pYieldType||chr(13) ||'pProgramList='||pProgramList||chr(13) ||'pPeriodType='||pPeriodType||chr(13) ||'pPeriodStart='||pPeriodStart||chr(13) ||'pPeriodEnd='||pPeriodEnd||chr(13) ||'pRowNumberOffset='||pRowNumberOffset||chr(13) ||chr(13);
	    	raise notice '%',noticeOutput;
			
			sqlCalendar := 'SELECT
    			t.CALENDAR_DATE_DISPLAY_NAME
    			,'||quote_literal(pPeriodType) ||' AS CALENDAR_PERIOD_TYPE
    			,t.CALENDAR_PERIOD_START_DATE
    			,t.CALENDAR_PERIOD_END_DATE
    			,'||quote_literal(pSiteCode) ||' AS CALENDAR_SITE_CODE
    			,t.CALENDAR_FISCAL_YEAR
    			,t.CALENDAR_FISCAL_QUARTER
    			,t.CALENDAR_WORK_WEEK
    			,t.CALENDAR_DATE
    			,t.CALENDAR_PERIOD_KEY --to be used for joining with actual results
    			FROM (
    				SELECT ' ||
		                CASE
		                WHEN pPeriodType='Quarter' THEN '''Q''||NVL(FISCAL_QUARTER,'''')||''''''''||SUBSTRING(NVL(FISCAL_YEAR,''''),3) AS CALENDAR_DATE_DISPLAY_NAME
		                							,FISCAL_YEAR||''Q''||FISCAL_QUARTER AS CALENDAR_PERIOD_KEY
		                							,ROW_NUMBER() OVER (PARTITION BY FISCAL_YEAR,FISCAL_QUARTER ORDER BY CURR_DATE ASC) AS CALENDAR_DATE_ASC_ORDER
		                							,MIN(CURR_DATE) OVER (PARTITION BY FISCAL_YEAR,FISCAL_QUARTER) CALENDAR_PERIOD_START_DATE
		                							,MAX(CURR_DATE) OVER (PARTITION BY FISCAL_YEAR,FISCAL_QUARTER) CALENDAR_PERIOD_END_DATE
		                ' WHEN pPeriodType='Week' THEN '''W''||NVL(FISCAL_WEEK,'''')||''Q''||NVL(FISCAL_QUARTER,'''')||''''''''||SUBSTRING(NVL(FISCAL_YEAR,''''),3) CALENDAR_DATE_DISPLAY_NAME
		                							,FISCAL_YEAR||''W''||FISCAL_WEEK CALENDAR_PERIOD_KEY
		                							,ROW_NUMBER() OVER (PARTITION BY FISCAL_YEAR,FISCAL_WEEK ORDER BY CURR_DATE ASC) CALENDAR_DATE_ASC_ORDER
		                							,MIN(CURR_DATE) OVER (PARTITION BY FISCAL_YEAR,FISCAL_WEEK) CALENDAR_PERIOD_START_DATE
		                							,MAX(CURR_DATE) OVER (PARTITION BY FISCAL_YEAR,FISCAL_WEEK) CALENDAR_PERIOD_END_DATE
		                ' WHEN pPeriodType='Day' THEN 'TO_CHAR(CURR_DATE,''MM/DD/YYYY'') CALENDAR_DATE_DISPLAY_NAME
		                							,FISCAL_YEAR||''D''||TO_CHAR(CURR_DATE,''YYYYMMDD'') CALENDAR_PERIOD_KEY
		                							,ROW_NUMBER() OVER (PARTITION BY CURR_DATE ORDER BY NULL) CALENDAR_DATE_ASC_ORDER
		                							,MIN(CURR_DATE) OVER (PARTITION BY CURR_DATE) CALENDAR_PERIOD_START_DATE
		                							,MAX(CURR_DATE) OVER (PARTITION BY CURR_DATE) CALENDAR_PERIOD_END_DATE
		                							' ELSE 'NULL' END || '
    					,FISCAL_YEAR AS CALENDAR_FISCAL_YEAR
    					,FISCAL_QUARTER AS CALENDAR_FISCAL_QUARTER
    					,FISCAL_WEEK AS CALENDAR_WORK_WEEK
    					,CURR_DATE AS  CALENDAR_DATE
    					FROM REP.BI_CALENDAR 
    					WHERE CURR_DATE BETWEEN TO_DATE('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') AND TO_DATE('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||
						')t WHERE t.CALENDAR_DATE_ASC_ORDER = 1' ;
		       

    		raise notice 'sqlCalendar=%',sqlCalendar;

    		sqlYieldTarget := 'SELECT
    			t.SITE_CODE AS TARGET_SITE_CODE
    			,t.LEVEL AS TARGET_LEVEL
    			,t.FISCAL_YEAR AS TARGET_FISCAL_YEAR
    			,t.FISCAL_QUARTER AS TARGET_FISCAL_QUARTER
    			,CASE WHEN t.NULL_YIELD_TARGET_COUNT=0 THEN t.YIELD_TARGET ELSE NULL END AS TARGET_YIELD_TARGET
    			FROM (
          			SELECT j.*
          			,COUNT(1) OVER (PARTITION BY j.SITE_CODE, j.LEVEL, j.FISCAL_YEAR, j.FISCAL_QUARTER) AS YIELD_TARGET_PER_QUARTER_COUNT
          			,SUM(CASE WHEN j.YIELD_TARGET IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY j.SITE_CODE) AS NULL_YIELD_TARGET_COUNT
          			FROM (
          			     SELECT DISTINCT p.SITE_CODE,t.LEVEL,t.FISCAL_YEAR,t.FISCAL_QUARTER,t.YIELD_TARGET
                     FROM (
                      			SELECT DISTINCT S.SITE_CODE, DP.PROGRAM_NAME
								FROM ADS.VIEW_DIM_PART DP
								LEFT JOIN ADS.VIEW_DIM_SITE S ON DP.SITE_KEY = S.SITE_KEY AND DP.DELETED_FLAG = ''N'' ::"VARCHAR" AND S.DELETED_FLAG = ''N'' ::"VARCHAR"
                      			WHERE S.SITE_CODE='||quote_literal(pSiteCode)||' ' ||
                          CASE
                          WHEN LENGTH(pProgramList) > 0 THEN ' AND DP.PROGRAM_NAME IN ('|| pProgramList ||') ' ELSE '' END || ') p ' || ' LEFT OUTER JOIN REP.PSYIELD_RULE_YIELD_TARGET t ON p.SITE_CODE=t.SITE_CODE AND p.PROGRAM_NAME=t.PROGRAM
                          			AND t.LEVEL='||quote_literal(pYieldGroup) || '
                      			) j
    			) t WHERE t.YIELD_TARGET_PER_QUARTER_COUNT=1 AND t.FISCAL_YEAR IS NOT NULL AND t.FISCAL_QUARTER IS NOT NULL';

    		raise notice 'sqlYieldTarget=%',sqlYieldTarget;

    		sqlRulesOperation := 'SELECT
		    			SITE_CODE AS RULES_SITE_CODE
		    			,OPERATION_NAME AS RULES_OPERATION_NAME
		    			,LOT_HISTORY_SITE_CODE AS RULES_TX_LOT_HISTORY_SITE_CODE
		    			,OPERATION_NAME AS RULES_RPTG_OPERATION_NAME
		    			,RULE_NAME AS RULES_PRIMARY_RULE_NAME
		    			,RULE_NAME_02 AS RULES_02_RULE_NAME
		    			,RULE_NAME_DEFECT AS RULES_DEFECT_RULE_NAME
		    			,INCLUDE_PN_SUFFIX_FLAG AS RULES_PROGRAM_WITH_PN_SUFFIX_FLAG
		    			' ||
		          CASE
		              WHEN pYieldGroup LIKE '4%' THEN ',PROCESS_GROUP_ID AS RULES_RPTG_AGG_GROUP_ID, PROCESS_GROUP AS RULES_RPTG_AGG_GROUP' 
					  WHEN pYieldGroup LIKE '3%' THEN ',PROCESS_TYPE_ID AS RULES_RPTG_AGG_GROUP_ID, PROCESS_TYPE AS RULES_RPTG_AGG_GROUP' 
					  WHEN pYieldGroup LIKE '2%' THEN ',YIELD_GROUP_ID AS RULES_RPTG_AGG_GROUP_ID, YIELD_GROUP AS RULES_RPTG_AGG_GROUP'
		              ELSE ',OVERALL_GROUP_ID AS RULES_RPTG_AGG_GROUP_ID, OVERALL_GROUP AS RULES_RPTG_AGG_GROUP' END || ' FROM REP.PSYIELD_RULE_YIELD_OPERATION WHERE DELETED_FLAG = ''N''::"VARVHAR" AND SITE_CODE=' || quote_literal(pSiteCode) ||
		          CASE
		          WHEN pYieldGroup
		          LIKE ( '4%' ) THEN ' and PROCESS_GROUP_ID like ' || quote_literal(pYieldGroup||'%')
		          WHEN pYieldGroup
		          LIKE ( '3%' ) THEN ' and PROCESS_TYPE_ID like ' || quote_literal(pYieldGroup||'%')
		          WHEN pYieldGroup
		          LIKE ( '2%' ) THEN ' and YIELD_GROUP_ID like ' || quote_literal(pYieldGroup||'%') ELSE ' ' END;

    		raise notice 'sqlRulesOperation=%',sqlRulesOperation;

		    if pPeriodType='Quarter' then pPeriodFields := ',M_LH_OPERATION_YEAR,M_LH_OPERATION_QUARTER';
		                                  pPeriodFieldKey := ',M_LH_OPERATION_YEAR||''Q''||M_LH_OPERATION_QUARTER ';
		    elsif pPeriodType='Week' then pPeriodFields := ',M_LH_OPERATION_YEAR,M_LH_OPERATION_WEEK';
		                                  pPeriodFieldKey := ',M_LH_OPERATION_YEAR||''W''||M_LH_OPERATION_WEEK ';
		    elsif pPeriodType='Day' then  pPeriodFields := ',M_LH_OPERATION_YEAR,M_LH_OPERATION_DATE';
		                                  pPeriodFieldKey := ',M_LH_OPERATION_YEAR||''D''||TO_CHAR(M_LH_OPERATION_DATE,''yyyymmdd'') ';
		    else  pPeriodFields := '';
		          pPeriodFieldKey := '';
		    end if;

   			sqlDefectLog := 'SELECT
		    			t.SITE_CODE AS DEFECT_SITE_CODE
		    			'||pPeriodFieldKey||' AS DEFECT_PERIOD_KEY
		    			,o.OPERATION_NAME AS  DEFECT_RPTG_OPERATION_NAME
		    			,SUM(t.SUM_DEFECT * r.DEFECT_FACTOR ) AS SUM_DEFECT
		    			FROM REP.PSYIELD_DEFECT_LOG_AGG t
		    			JOIN REP.PSYIELD_RULE_YIELD_DEFECT r ON t.M_LH_OLD_OPERATION_NAME = r.OPERATION_NAME AND t.SITE_CODE = r.SITE_CODE AND t.DEFECT_CODE = r.DEFECT_CODE
		    			JOIN REP.PSYIELD_RULE_YIELD_OPERATION o ON o.SITE_CODE ='||quote_literal(pSiteCode)||' AND r.RULE_NAME =o.RULE_NAME_DEFECT
		    			WHERE
		        			--same condition as below!
		        			t.IS_TEST_LOT_ID_FLAG = ''N''::"VARCHAR"
		        			AND t.M_LH_DELETED_FLAG = ''N''::"VARCHAR"
		        			AND t.M_LH_GROUP_FLAG, = ''N''::"VARCHAR"
		        			--AND t.M_LH_TRANSACTION_NAME IN (SELECT RULE_NAME from REP.PSYIELD_RULE_YIELD_TRANS WHERE DELETED_FLAG = ''N''::"VARCHAR"
							) ' ||
		          CASE
		          WHEN LENGTH(pProgramList) > 0 THEN ' AND t.M_LH_OLD_PROGRAM_NAME  IN ('|| pProgramList ||') ' ELSE '' END || '
		          			AND ( o.INCLUDE_PN_SUFFIX_FLAG IS NULL OR o.INCLUDE_PN_SUFFIX_FLAG = t.M_LH_OLD_PROGRAM_WITH_PN_SUFFIX_FLAG )
		          			--date range
		          			AND t.M_LH_OPERATION_DATE BETWEEN TO_DATE('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') AND TO_DATE('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')
		          			--owner and facility location
		          			AND (r.OWNER_FACILITY_RULE_NAME,t.M_LH_OWNER) IN (SELECT DISTINCT RULE_NAME, OWNER from REP.PSYIELD_RULE_YIELD_OWNER WHERE DELETED_FLAG = ''N''::"VARCHAR" AND YIELD_TYPE ='||quote_literal(pYieldType)||' )
		          			AND (r.OWNER_FACILITY_RULE_NAME,t.SITE_CODE, t.M_LH_OLD_FACILITY) IN (SELECT DISTINCT RULE_NAME, SITE_CODE, FACILITY FROM REP.PSYIELD_RULE_YIELD_FACILITY  )
		          			GROUP BY t.SITE_CODE '||pPeriodFields||'
		          			,o.REPORT_OPERATION_NAME';

    		raise notice 'sqlDefectLog=%',sqlDefectLog;

			sqlReworkLoss := 'SELECT
						rl.SITE_CODE AS REWORKLOSS_SITE_CODE
						'||pPeriodFieldKey||' AS  REWORKLOSS_PERIOD_KEY
						,rules.RULES_RPTG_OPERATION_NAME AS REWORKLOSS_RPTG_OPERATION_NAME
						,SUM(rl.SUM_LOSS) AS SUM_REWORKLOSS
						FROM REP.PSYIELD_LOT_HIS_AGG rl
							JOIN ('|| sqlRulesOperation ||' ) rules
								ON rl.M_LH_OLD_ROUTE_NAME_REWORK_SUFFIX IS NOT NULL AND rl.SITE_CODE = rules.RULES_TX_LOT_HISTORY_SITE_CODE and rl.M_LH_OLD_ROUTE_NAME_REWORK_SUFFIX = rules.RULES_OPERATION_NAME AND LENGTH(rules.RULES_RPTG_AGG_GROUP_ID) > 0
						WHERE
						--same condition as below!
			      			rl.IS_TEST_LOT_ID_FLAG = ''N''::"VARCHAR"
			      			AND rl.M_LH_DELETED_FLAG = ''N''::"VARCHAR"
			      			AND rl.M_LH_GROUP_FLAG = ''N''::"VARCHAR"
			      			--AND rl.M_LH_TRANSACTION_NAME IN (SELECT RULE_NAME from REP.PSYIELD_RULE_YIELD_TRANS WHERE DELETED_FLAG = ''N''::"VARCHAR"
							) ' ||
			      CASE
			      WHEN LENGTH(pProgramList) > 0 THEN ' and rl.M_LH_OLD_PROGRAM_NAME IN ('|| pProgramList ||') ' ELSE '' END || '
			      			AND ( rules.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG IS NULL OR rules.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG = rl.M_LH_OLD_PROGRAM_WITH_PN_SUFFIX_FLAG )

			      			--date range
			      			AND rl.M_LH_OPERATION_DATE BETWEEN TO_DATE('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') AND TO_DATE('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')
			      			--owner and facility location
			      			AND
			      			(
			      				-- RULES_PRIMARY_RULE_NAME
			      				(
			      					(rules.RULES_PRIMARY_RULE_NAME, rl.M_LH_OWNER) IN (SELECT DISTINCT RULE_NAME,OWNER FROM REP.REP_RULE_YIELD_OWNER WHERE DELETED_FLAG = ''N''::"VARCHAR" AND DENOMINATOR_COUNT_REWORK_LOSS=1 AND YIELD_TYPE='||quote_literal(pYieldType)||' )
			      					AND (rules.RULES_PRIMARY_RULE_NAME, rl.SITE_CODE, rl.M_LH_OLD_FACILITY) IN (SELECT DISTINCT RULE_NAME, SITE_CODE, FACILITY FROM REP.REP_RULE_YIELD_FACILITY WHERE DELETED_FLAG = ''N''::"VARCHAR" AND DENOMINATOR_COUNT_REWORK_LOSS=1  )
			      				)
			      				OR
			      				-- RULES_02_RULE_NAME
			      				(
			      					(rules.RULES_02_RULE_NAME, rl.M_LH_OWNER) IN (SELECT DISTINCT RULE_NAME,OWNER FROM REP.REP_RULE_YIELD_OWNER WHERE DELETED_FLAG = ''N''::"VARCHAR" AND DENOMINATOR_COUNT_REWORK_LOSS=1 AND YIELD_TYPE='||quote_literal(pYieldType)||' )
			      					AND (rules.RULES_02_RULE_NAME, rl.SITE_CODE, rl.M_LH_OLD_FACILITY) IN (SELECT DISTINCT RULE_NAME, SITE_CODE, FACILITY FROM REP.REP_RULE_YIELD_FACILITY WHERE DELETED_FLAG = ''N''::"VARCHAR" AND DENOMINATOR_COUNT_REWORK_LOSS=1)
			      				)
			      			)
			      			GROUP BY rl.SITE_CODE '||pPeriodFields||'
			      			,rules.RULES_RPTG_OPERATION_NAME';

			raise notice 'sqlReworkLoss=%',sqlReworkLoss;

    		sqlYieldActual := 'SELECT
    			--m_lh_site_code actual_site_code
    			RULES_SITE_CODE AS ACTUAL_SITE_CODE
    			,RULES_RPTG_AGG_GROUP_ID AS ACTUAL_YIELD_GROUP
    			,ACTUAL_PERIOD_KEY
    			,EXP(SUM(LN(RPTG_OPERATION_YIELD))) AS ACTUAL_YIELD_ACTUAL
    			,CASE WHEN LENGTH(MIN(ACTUAL_COMMENTS)) > 0 THEN TRUE ELSE FALSE END AS ACTUAL_HAS_COMMENTS_FLAG
    			,MIN(ACTUAL_COMMENTS) AS ACTUAL_COMMENTS
    			FROM ( --Begins ACTUAL_YIELD_STEP_04_ZERO_DENOMINATOR_AND_NUMERATOR_HANDLING_WITHOUT_AGGREGATION
          				select
          				SITE_CODE
          				,RULES_SITE_CODE
          				,RULES_RPTG_AGG_GROUP_ID
          				,RULES_RPTG_OPERATION_NAME
          				,ACTUAL_PERIOD_KEY
          				,CASE WHEN DENOMINATOR_PER_OPERATION <= 0 OR NUMERATOR_PER_OPERATION <= 0 THEN 1.0 ELSE CAST(NUMERATOR_PER_OPERATION AS DOUBLE)/DENOMINATOR_PER_OPERATION END AS RPTG_OPERATION_YIELD
          				,CASE WHEN NUMERATOR_PER_OPERATION <= 0 THEN ''OPERATION ''|| RULES_RPTG_OPERATION_NAME || ' || quote_literal(' HAS INVALID TOTAL OUT AMOUNT=') ||'||NUMERATOR_PER_OPERATION||'||quote_literal('. ')||' ELSE NULL END ACTUAL_COMMENTS
          				FROM
        					(--Begins ACTUAL_YIELD_STEP_03_DEFECT_AND_REWORK_ROUTE_HANDLING
            						SELECT
            						SITE_CODE
            						,ACTUAL_PERIOD_KEY
            						,RULES_SITE_CODE
            						,RULES_RPTG_AGG_GROUP_ID
            						,RULES_RPTG_OPERATION_NAME
            						--STEP_03 aggregated fields
            						,NVL(NUMERATOR_PER_OPERATION,0) NUMERATOR_PER_OPERATION
            						,(
            							NVL(DENOMINATOR_PER_OPERATION,0)
            							+ NVL(SUM_DEFECT,0)
            							+ NVL(SUM_REWORKLOSS,0)
            						) DENOMINATOR_PER_OPERATION
            						--ends STEP_03 aggregated fields
            						FROM
            						(--Begins ACTUAL_YIELD_STEP_02_RULE_EXECUTION_ON_OWNER_LOCATION_ON_MAIN_OPERATION_NAME
              							SELECT
              							SITE_CODE
              							,ACTUAL_PERIOD_KEY
              							,RULES_SITE_CODE
              							,RULES_RPTG_AGG_GROUP_ID
              							,RULES_RPTG_OPERATION_NAME
              							--STEP_02 aggregated fields
              							,(
              								NVL(SUM(SUM_OUT * rules01o.NUMERATOR_COUNT_OUT * rules01f.NUMERATOR_COUNT_OUT ),0)

              								+ NVL(SUM(SUM_OUT * rules02o.NUMERATOR_COUNT_OUT * rules02f.NUMERATOR_COUNT_OUT ),0)
              							) NUMERATOR_PER_OPERATION
              							,(
              								NVL(SUM(SUM_OUT * rules01o.DENOMINATOR_COUNT_OUT * rules01f.DENOMINATOR_COUNT_OUT ),0)
              								+ NVL(SUM(SUM_LOSS * rules01o.DENOMINATOR_COUNT_LOSS * rules01f.DENOMINATOR_COUNT_LOSS),0)

              								+ NVL(SUM(SUM_OUT * rules02o.DENOMINATOR_COUNT_OUT * rules02f.DENOMINATOR_COUNT_OUT ),0)
              								+ NVL(SUM(SUM_LOSS * rules02o.DENOMINATOR_COUNT_LOSS * rules02f.DENOMINATOR_COUNT_LOSS),0)
              							) DENOMINATOR_PER_OPERATION
              							--ends STEP_02 aggregated fields
              							FROM
              							( --Begins ACTUAL_YIELD_STEP_01_DATA_SELECTION_AND_AGGREGATION_PRESERVING_OWNER_AND_FACILITY_LOCATION
                  								SELECT
                  								lh.SITE_CODE
                  								'||pPeriodFieldKey||' AS ACTUAL_PERIOD_KEY
                  								,lh.M_LH_OWNER
                  								,lh.M_LH_OLD_FACILITY
                  								--all rule fields have to go in here as well as the GROUP BY below
                  								,rules.RULES_SITE_CODE
                  								,rules.RULES_RPTG_AGG_GROUP_ID
                  								,rules.RULES_RPTG_OPERATION_NAME
                  								,rules.RULES_PRIMARY_RULE_NAME
                  								,rules.RULES_02_RULE_NAME
                  								--ends rule fields
                  								--STEP_01 aggregated fields (still preserving the owner and facility_location)
                  								,SUM(lh.SUM_IN) AS SUM_IN
                  								,SUM(lh.SUM_OUT) AS SUM_OUT
                  								,SUM(lh.SUM_LOSS) AS SUM_LOSS
                  								--ends STEP_01 aggregated fields
                  								FROM REP.PSYIELD_LOT_HIS_AGG lh
                  								JOIN ('|| sqlRulesOperation ||') rules
                  									ON lh.SITE_CODE rules.RULES_TX_LOT_HISTORY_SITE_CODE AND lh.M_LH_OLD_OPERATION_NAME = rules.RULES_OPERATION_NAME AND LENGTH(rules.RULES_RPTG_AGG_GROUP_ID) > 0
                  								WHERE
                  								lh.IS_TEST_LOT_ID_FLAG = ''N''::"VARCHAR"
                  								AND lh.M_LH_DELETED_FLAG  = ''N''::"VARCHAR"
                  								AND lh.M_LH_GROUP_FLAG = ''N''::"VARCHAR"
                  								--AND lh.M_LH_TRANSACTION_NAME IN (SELECT RULE_NAME from REP.PSYIELD_RULE_YIELD_TRANS WHERE DELETED_FLAG = ''N''::"VARCHAR"
												) ' ||
                              CASE
                              WHEN LENGTH(pProgramList) > 0 THEN ' AND lh.M_LH_OLD_PROGRAM_NAME IN ('|| pProgramList ||') ' ELSE '' END || '
                  								--the M_LH_OLD_PRODUCT_SK_ID_PROGRAM_WITH_PN_SUFFIX_FLAG rule
                  								AND (rules.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG IS NULL OR rules.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG = lh.M_LH_OLD_PROGRAM_WITH_PN_SUFFIX_FLAG )
                  								--date range
                  								AND lh.M_LH_OPERATION_DATE BETWEEN TO_DATE('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') AND TO_DATE('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')

                  								--the following two clauses are for 1st-round filtering.  additional rules are in step 02
                  								AND
                  								(
                  									(
                  										(rules.RULES_PRIMARY_RULE_NAME,lh.M_LH_OWNER) IN (SELECT DISTINCT RULE_NAME,OWNER from REP.REP_RULE_YIELD_OWNER WHERE DELETED_FLAG = ''N''::"VARCHAR" AND YIELD_TYPE='||quote_literal(pYieldType)||' )
                  										AND (RULES_PRIMARY_RULE_NAME, lh.SITE_CODE, lh.M_LH_OLD_FACILITY ) IN (SELECT DISTINCT RULE_NAME, SITE_CODE, FACILITY FROM REP.REP_RULE_YIELD_FACILITY WHERE DELETED_FLAG = ''N''::"VARCHAR" )
                  									)
                  									OR
                  									(
                  										(rules.RULES_02_RULE_NAME,lh.M_LH_OWNER) IN (SELECT DISTINCT RULE_NAME,OWNER from REP.REP_RULE_YIELD_OWNER WHERE DELETED_FLAG = ''N''::"VARCHAR" AND YIELD_TYPE='||quote_literal(pYieldType)||'  )
                  										AND (RULES_02_RULE_NAME,lh.SITE_CODE,lh.M_LH_OLD_FACILITY) IN (SELECT DISTINCT RULE_NAME, SITE_CODE, FACILITY FROM REP.REP_RULE_YIELD_FACILITY WHERE DELETED_FLAG = ''N''::"VARCHAR"  )
                  									)
                  								)
                  								GROUP BY lh.SITE_CODE '||pPeriodFields||',lh.M_LH_OWNER,lh.M_LH_OLD_FACILITY

                  								--all rule fields have to go in here as well as in the SELECT above
                  									,rules.RULES_SITE_CODE
                  									,rules.RULES_RPTG_AGG_GROUP_ID
                  									,rules.RULES_RPTG_OPERATION_NAME
                  									,rules.RULES_PRIMARY_RULE_NAME
                  									,rules.RULES_02_RULE_NAME
                  								--ends rule fields
                  					) --ends ACTUAL_YIELD_STEP_01_DATA_SELECTION_AND_AGGREGATION_PRESERVING_OWNER_AND_FACILITY_LOCATION
									AGG
              							LEFT OUTER JOIN REP.REP_RULE_YIELD_OWNER rules01o
              								ON AGG.RULES_PRIMARY_RULE_NAME = rules01o.RULE_NAME AND AGG.M_LH_OWNER = rules01o.OWNER AND rules01o.YIELD_TYPE='||quote_literal(pYieldType)||'
              							LEFT OUTER JOIN REP.REP_RULE_YIELD_FACILITY rules01f
              								ON AGG.RULES_PRIMARY_RULE_NAME = rules01f.RULE_NAME AND AGG.M_LH_OLD_FACILITY = rules01f.FACILITY AND AGG.SITE_CODE = rules01f.SITE_CODE
              							LEFT OUTER join rptg_lu_yield_rules_owner rules02o
              								on AGG.RULES_02_RULE_NAME = rules02o.RULE_NAME AND AGG.M_LH_OWNER=rules02o.OWNER AND rules02o.YIELD_TYPE='||quote_literal(pYieldType)||'
              							LEFT OUTER join RPTG_LU_YIELD_RULES_FACILITY_LOCATION rules02f
              								on AGG.RULES_02_RULE_NAME = rules02f.RULE_NAME AND AGG.M_LH_OLD_FACILITY = rules02f.FACILITY and AGG.SITE_CODE = rules02f.SITE_CODE
              							GROUP BY AGG.SITE_CODE, AGG.ACTUAL_PERIOD_KEY, AGG.RULES_SITE_CODE, AGG.RULES_RPTG_AGG_GROUP_ID, AGG.RULES_RPTG_OPERATION_NAME
    						      ) --ACTUAL_YIELD_STEP_02_RULE_EXECUTION_ON_OWNER_LOCATION_ON_MAIN_OPERATION_NAME
								  MAIN
          						LEFT OUTER JOIN ('|| sqlDefectLog ||') dl
          							ON MAIN.SITE_CODE= dl.DEFECT_SITE_CODE
          								AND MAIN.ACTUAL_PERIOD_KEY= dl.DEFECT_PERIOD_KEY
          								AND MAIN.RULES_RPTG_OPERATION_NAME = dl.DEFECT_RPTG_OPERATION_NAME
          						LEFT OUTER JOIN ('|| sqlReworkLoss || ') rl
          							ON MAIN.M_LH_SITE_CODE= rl.REWORKLOSS_SITE_CODE
          								AND MAIN.ACTUAL_PERIOD_KEY = rl.REWORKLOSS_PERIOD_KEY
          								AND MAIN.RULES_RPTG_OPERATION_NAME = rl.REWORKLOSS_RPTG_OPERATION_NAME
    					    ) REW
							--ACTUAL_YIELD_STEP_03_DEFECT_AND_REWORK_ROUTE_HANDLING
    			    ) HAND
					--ACTUAL_YIELD_STEP_04_ZERO_DENOMINATOR_AND_NUMERATOR_HANDLING_WITHOUT_AGGREGATION
    			-- and here is the final grouping for multiplication for actual yield
    			--group by m_lh_site_code,RULES_RPTG_AGG_GROUP_ID,actual_period_key
    			   GROUP BY RULES_SITE_CODE,RULES_RPTG_AGG_GROUP_ID,ACTUAL_PERIOD_KEY';

			raise notice 'sqlYieldActual=%',sqlYieldActual;

			sqlFinal := 'INSERT INTO '||REFTABLENAME||' SELECT
					(ROW_NUMBER() OVER (ORDER BY CALENDAR_SITE_CODE,CALENDAR_DATE,YIELD_GROUP_RPTG_AGG_GROUP_ID) + '||pRowNumberOffset||') CALENDAR_ROW_NUMBER
					,CALENDAR_DATE_DISPLAY_NAME
					,CALENDAR_PERIOD_TYPE
					,CALENDAR_PERIOD_START_DATE
					,CALENDAR_PERIOD_END_DATE
					,ACTUAL_YIELD_ACTUAL
					,TARGET_YIELD_TARGET
					,CALENDAR_SITE_CODE
					,YIELD_GROUP_RPTG_AGG_GROUP_ID
					,'||quote_literal(pYieldType)||'
					,NVL(ACTUAL_HAS_COMMENTS_FLAG,FALSE)
					, ACTUAL_COMMENTS
					FROM ('||sqlCalendar||') FINAL_JOIN_CALENDAR
					JOIN (SELECT DISTINCT RULES_SITE_CODE YIELD_GROUP_SITE_CODE, RULES_RPTG_AGG_GROUP_ID YIELD_GROUP_RPTG_AGG_GROUP_ID  FROM ('||sqlRulesOperation||') R WHERE LENGTH(RULES_RPTG_AGG_GROUP_ID) > 0 ) FINAL_JOIN_YIELD_GROUP ON CALENDAR_SITE_CODE=YIELD_GROUP_SITE_CODE
					LEFT OUTER JOIN ( '||sqlYieldTarget||' ) FINAL_JOIN_TARGET ON CALENDAR_SITE_CODE=TARGET_SITE_CODE AND CALENDAR_FISCAL_YEAR=TARGET_FISCAL_YEAR AND CALENDAR_FISCAL_QUARTER=TARGET_FISCAL_QUARTER
					LEFT OUTER JOIN ( '||sqlYieldActual||' ) FINAL_JOIN_ACTUAL
						ON CALENDAR_SITE_CODE=ACTUAL_SITE_CODE
						AND YIELD_GROUP_RPTG_AGG_GROUP_ID = ACTUAL_YIELD_GROUP
						AND CALENDAR_PERIOD_KEY = ACTUAL_PERIOD_KEY';

			raise notice 'sqlFinal=%',sqlFinal;
			
			EXECUTE IMMEDIATE sqlFinal;
			
			RETURN REFTABLE;
		
		END;
	END;
END_PROC;

