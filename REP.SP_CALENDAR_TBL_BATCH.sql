CREATE OR REPLACE PROCEDURE REP.SP_CALENDAR_TBL_BATCH(
BIGINT
)
RETURNS INTEGER
EXECUTE AS OWNER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	DECLARE 
		pSchHrs ALIAS FOR $1;
		
		pExistFlag boolean;
		pTableName varchar;
		pTableSchema varchar;
		pBatchCalendarView varchar;
BEGIN
	BEGIN AUTOCOMMIT ON
	
		pTableName := 'BI_CALENDAR_TMP';
		pTableSchema := 'REP';
		
		pBatchCalendarView :='
					SELECT DISTINCT TMP.CURR_DATE, 
							FD_D.PERIOD_NUMBER AS FISCAL_DAY, FD_D.PERIOD_START_DATE_TIME AS FISCAL_DAY_START_TIME, FD_D.PERIOD_END_DATE_TIME AS FISCAL_DAY_END_TIME,
							FD_W.PERIOD_NUMBER AS FISCAL_WEEK, FD_W.PERIOD_START_DATE_TIME AS FISCAL_WEEK_START_TIME, FD_W.PERIOD_END_DATE_TIME AS FISCAL_WEEK_END_TIME,
							FD_M.PERIOD_NUMBER AS FISCAL_MONTH, FD_M.PERIOD_START_DATE_TIME AS FISCAL_MONTH_START_TIME, FD_M.PERIOD_END_DATE_TIME AS FISCAL_MONTH_END_TIME,
							FD_Q.PERIOD_NUMBER AS FISCAL_QUARTER, FD_Q.PERIOD_START_DATE_TIME AS FISCAL_QUARTER_START_TIME, FD_Q.PERIOD_END_DATE_TIME AS FISCAL_QUARTER_END_TIME, 
							FD_Y.PERIOD_NUMBER AS FISCAL_YEAR, FD_Y.PERIOD_START_DATE_TIME AS FISCAL_YEAR_START_TIME, FD_Y.PERIOD_END_DATE_TIME AS FISCAL_YEAR_END_TIME
					from (
					 		(
						    select CURRENT_DATE + INTERVAL ( (a.a + (10 * b.a) + (100 * c.a)) || ''DAYS'')  as CURR_DATE
						    from (
								select 0 as a 
								union all select 1 
								union all select 2 
								union all select 3
								union all select 4 
								union all select 5 
								union all select 6 
								union all select 7 
								union all select 8 
								union all select 9
							) as a
					    	cross join (
										select 0 as a 
										union all select 1 
										union all select 2 
										union all select 3 
										union all select 4 
										union all select 5 
										union all select 6 
										union all select 7 
										union all select 8 
										union all select 9
									) as b
					    	cross join (
										select 0 as a 
										union all select 1 
										union all select 2 
										union all select 3 
										union all select 4 
										union all select 5 
										union all select 6 
										union all select 7 
										union all select 8 
										union all select 9
									) as c
							) 
					UNION ALL 
						(
						select CURRENT_DATE - INTERVAL ( (a.a + (10 * b.a) + (100 * c.a)) || ''DAYS'')  as CURR_DATE
				    	from (
							select 0 as a 
							union all select 1 
							union all select 2 
							union all select 3
							union all select 4 
							union all select 5 
							union all select 6 
							union all select 7 
							union all select 8 
							union all select 9
						) as a
				    	cross join (
									select 0 as a 
									union all select 1 
									union all select 2 
									union all select 3 
									union all select 4 
									union all select 5 
									union all select 6 
									union all select 7 
									union all select 8 
									union all select 9
								) as b
				    	cross join (
									select 0 as a 
									union all select 1 
									union all select 2 
									union all select 3 
									union all select 4 
									union all select 5 
									union all select 6 
									union all select 7 
									union all select 8 
									union all select 9
								) as c
						) 
					) TMP
					LEFT JOIN ADS.VIEW_DIM_DATE FD_D ON (TMP.CURR_DATE + INTERVAL('||pSchHrs||' ||''hours'') >= FD_D.PERIOD_START_DATE_TIME) AND (TMP.CURR_DATE <= FD_D.PERIOD_END_DATE_TIME) AND (FD_D.SOURCE_SYSTEM_CODE = ''SYSTEM'' ) AND (FD_D.DELETED_FLAG = ''N'') AND (FD_D.PERIOD_TYPE = ''D'')
					LEFT JOIN ADS.VIEW_DIM_DATE FD_W ON (TMP.CURR_DATE + INTERVAL('||pSchHrs||' ||''hours'') >= FD_W.PERIOD_START_DATE_TIME) AND (TMP.CURR_DATE <= FD_W.PERIOD_END_DATE_TIME) AND (FD_W.SOURCE_SYSTEM_CODE != ''SYSTEM'' ) AND (FD_W.DELETED_FLAG = ''N'') AND (FD_W.PERIOD_TYPE = ''W'')
					LEFT JOIN ADS.VIEW_DIM_DATE FD_M ON (TMP.CURR_DATE + INTERVAL('||pSchHrs||' ||''hours'') >= FD_M.PERIOD_START_DATE_TIME) AND (TMP.CURR_DATE <= FD_M.PERIOD_END_DATE_TIME) AND (FD_M.SOURCE_SYSTEM_CODE != ''SYSTEM'' ) AND (FD_M.DELETED_FLAG = ''N'') AND (FD_M.PERIOD_TYPE = ''M'')
					LEFT JOIN ADS.VIEW_DIM_DATE FD_Q ON (TMP.CURR_DATE + INTERVAL('||pSchHrs||' ||''hours'') >= FD_Q.PERIOD_START_DATE_TIME) AND (TMP.CURR_DATE <= FD_Q.PERIOD_END_DATE_TIME) AND (FD_Q.SOURCE_SYSTEM_CODE != ''SYSTEM'' ) AND (FD_Q.DELETED_FLAG = ''N'') AND (FD_Q.PERIOD_TYPE = ''Q'')
					LEFT JOIN ADS.VIEW_DIM_DATE FD_Y ON (TMP.CURR_DATE + INTERVAL('||pSchHrs||' ||''hours'') >= FD_Y.PERIOD_START_DATE_TIME) AND (TMP.CURR_DATE <= FD_Y.PERIOD_END_DATE_TIME) AND (FD_Y.SOURCE_SYSTEM_CODE != ''SYSTEM'' ) AND (FD_Y.DELETED_FLAG = ''N'') AND (FD_Y.PERIOD_TYPE = ''Y'')
					WHERE FD_D.PERIOD_NUMBER IS NOT NULL OR FD_W.PERIOD_NUMBER IS NOT NULL  OR FD_M.PERIOD_NUMBER IS NOT NULL OR FD_Q.PERIOD_NUMBER IS NOT NULL OR FD_Y.PERIOD_NUMBER IS NOT NULL

		
		';
		
		--	RAISE NOTICE 'pBatchCalendarView=%', pBatchCalendarView;
			
		SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pExistFlag FROM _V_TABLE WHERE TABLENAME = pTableName AND SCHEMA = pTableSchema;
		
		IF pExistFlag  = TRUE THEN
			EXECUTE IMMEDIATE 'DROP TABLE ' || pTableSchema || '.'||pTableName;
			RAISE NOTICE 'Table %.% exist, deleted.', pTableSchema, pTableName;
		END IF;
		
		EXECUTE IMMEDIATE 'CREATE TABLE '|| pTableSchema || '.'||pTableName ||' AS SELECT * FROM ( ' || pBatchCalendarView || ' ) t DISTRIBUTE ON (CURR_DATE)';
		RAISE NOTICE 'create Table %.%, .', pTableSchema, pTableName;
	END;
END;
END_PROC;
