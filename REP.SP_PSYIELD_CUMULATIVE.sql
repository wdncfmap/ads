CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_CUMULATIVE(CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY))
RETURNS REFTABLE(MHO_ASIA_BI_DEV.REP.SPTBL_PSYIELD_CUMULATIVE)
LANGUAGE NZPLSQL AS
BEGIN_PROC
	 DECLARE 
	 	pInputSiteCode ALIAS FOR $1;
		pInputYieldGroup ALIAS FOR $2;
		pInputYieldType ALIAS FOR $3;
		pInputProgramList ALIAS FOR $4;
		pInputPeriodType ALIAS FOR $5;
		pInputPeriodStart ALIAS FOR $6;
		pInputPeriodEnd ALIAS FOR $7;

		pSiteCode varchar;
		pYieldGroup varchar;
		pYieldType varchar;
		pPeriodType varchar;
		pProgramList varchar;
		pPeriodStart date;
		pPeriodEnd date;
		pBusinessDate date;
		pRefTableRowCount bigint;
		sqlstmt varchar;
		noticeOutput varchar;
		
	BEGIN
		BEGIN AUTOCOMMIT ON
			
			noticeOutput := 'SP_PSYIELD_CUMULATIVE'||chr(13) ||'Raw parameters:'||chr(13) ||'pInputSiteCode='||pInputSiteCode||chr(13) ||'pInputYieldGroup='||pInputYieldGroup||chr(13) ||'pInputYieldType='||pInputYieldType||chr(13) ||'pInputProgramList='||pInputProgramList||chr(13) ||'pInputPeriodType='||pInputPeriodType||chr(13) ||'pInputPeriodStart='||pInputPeriodStart||chr(13) ||'pInputPeriodEnd='||pInputPeriodEnd||chr(13) ||chr(13);

		    pSiteCode := case when pInputSiteCode in ('WDB','PNS') then pInputSiteCode else 'WDB' end;

		    pYieldGroup := case when pInputYieldGroup in ('1','2','3','4','4A','4B') then pInputYieldGroup else '1' end;

		    pYieldType := case when pInputYieldType in ('Prime','Rework','Blended') then pInputYieldType else 'Blended' end;

		    if pInputProgramList = 'ALL' or length(trim(nvl(pInputProgramList,'')))=0
		      then pProgramList := '';
		    else pProgramList := SP_QUOTE_LITERAL_LIST(pInputProgramList,',');
		    end if;

		    pPeriodType := case when pInputPeriodType in ('Quarter','Week','Day','Default') then pInputPeriodType else 'Default' end;

		    if pPeriodType in ('Day','Week','Quarter') then 
		      	pPeriodStart := to_timestamp(pInputPeriodStart||chr(32)||'07:00:00','mm/dd/yyyy HH24:MI:SS');
		        pPeriodEnd := to_timestamp(pInputPeriodEnd||chr(32)||'07:00:00','mm/dd/yyyy HH24:MI:SS');

		        if pPeriodStart > pPeriodEnd then 
		           raise exception 'pPeriodStart cannot be later than pPeriodEnd.';
		        end if;

		      if pPeriodType='Week' then 
		        select DATE(D.PERIOD_START_DATE_TIME), DATE(D.PERIOD_END_DATE_TIME)
		                INTO pPeriodStart,pPeriodEnd
		                from ADS.VIEW_DIM_DATE D
						JOIN ADS.VIEW_DIM_SITE S ON D.SITE_KEY = S.SITE_KEY
		                where D.PERIOD_START_DATE_TIME >= pPeriodStart
		                      and D.PERIOD_END_DATE_TIME <= pPeriodEnd
							  and D.PERIOD_TYPE = 'W'::"VARCHAR" 
		                      and S.SITE_CODE =pSiteCode;
							  
		      elsif pPeriodType='Quarter'
		        then select DATE(D.PERIOD_START_DATE_TIME), DATE(D.PERIOD_END_DATE_TIME)
		                INTO pPeriodStart,pPeriodEnd
		                from ADS.VIEW_DIM_DATE D
						JOIN ADS.VIEW_DIM_SITE S ON D.SITE_KEY = S.SITE_KEY
		                where D.PERIOD_START_DATE_TIME >= pPeriodStart
		                      and D.PERIOD_END_DATE_TIME <= pPeriodEnd
							  and D.PERIOD_TYPE = 'Q'::"VARCHAR" 
		                      and S.SITE_CODE =pSiteCode;
		      end if;
		    end if;

		    pBusinessDate := current_timestamp;
		    noticeOutput := nvl(noticeOutput,'')||'Effective parameters:'||chr(13) ||'pSiteCode='||pSiteCode||chr(13) ||'pYieldGroup='||pYieldGroup||chr(13) ||'pYieldType='||pYieldType||chr(13) ||'pProgramList='||pProgramList||chr(13) ||'pPeriodType='||pPeriodType||chr(13) ||'pPeriodStart='||pPeriodStart||chr(13) ||'pPeriodEnd='||pPeriodEnd||chr(13);
		    raise notice '%',noticeOutput;

		    if pPeriodType in ('Day','Week','Quarter')
		      then sqlstmt := 'insert into '||REFTABLENAME||' select REP.SP_PSYIELD_CUMULATIVE_INTERNAL(' ||quote_literal(pSiteCode)||',' ||quote_literal(pYieldGroup)||',' ||quote_literal(pYieldType)||',' ||quote_literal(pProgramList)||',' ||quote_literal(pPeriodType)||',' ||quote_literal(to_char(pPeriodStart,'yyyy-mm-dd'))||',' ||quote_literal(to_char(pPeriodEnd,'yyyy-mm-dd'))||',' ||' 0 )';
		           EXECUTE IMMEDIATE sqlstmt;

		--*********************************************************************************************************
		-- For defaul one year yield calculation, modified after asking for details - 11/11/2015.
		--*********************************************************************************************************
		    else
		        SELECT DATE(MIN(PERIOD_START_DATE_TIME)), DATE(MAX(PERIOD_END_DATE_TIME))
				INTO pPeriodStart,pPeriodEnd
				FROM (
						SELECT ROW_NUMBER() OVER(ORDER BY PERIOD_YEAR DESC, PERIOD_NUMBER DESC) Y, PERIOD_START_DATE_TIME, PERIOD_END_DATE_TIME
					  	FROM MHO_ASIA_BI_DEV.ADS.VIEW_DIM_DATE
						WHERE PERIOD_END_DATE_TIME <= current_date AND PERIOD_TYPE = 'Q'
					) T
					WHERE T.Y <=3;
					
		      sqlstmt := 'insert into '||REFTABLENAME||' select REP.SP_PSYIELD_CUMULATIVE_INTERNAL(' ||quote_literal(pSiteCode)||',' ||quote_literal(pYieldGroup)||',' ||quote_literal(pYieldType)||',' ||quote_literal(pProgramList)||',' ||quote_literal('Quarter')||',' ||quote_literal(to_char(pPeriodStart,'yyyy-mm-dd'))||',' ||quote_literal(to_char(pPeriodEnd,'yyyy-mm-dd'))||',' ||' 0 )';
		      EXECUTE IMMEDIATE sqlstmt;

		      pRefTableRowCount := ROW_COUNT;

		      SELECT DATE(MIN(PERIOD_START_DATE_TIME)), DATE(MAX(PERIOD_END_DATE_TIME))
			  INTO pPeriodStart,pPeriodEnd
			  FROM (
					SELECT DENSE_RANK() OVER(ORDER BY PERIOD_YEAR DESC ) Y, PERIOD_YEAR, PERIOD_TYPE, PERIOD_NUMBER, PERIOD_START_DATE_TIME, PERIOD_END_DATE_TIME
				  	FROM MHO_ASIA_BI_DEV.ADS.VIEW_DIM_DATE
					WHERE PERIOD_END_DATE_TIME <= current_date  AND PERIOD_START_DATE_TIME >= pPeriodEnd AND PERIOD_TYPE = 'W' 
					) T

		      sqlstmt := 'insert into '||REFTABLENAME||' select REP.SP_PSYIELD_CUMULATIVE_INTERNAL(' ||quote_literal(pSiteCode)||',' ||quote_literal(pYieldGroup)||',' ||quote_literal(pYieldType)||',' ||quote_literal(pProgramList)||',' ||quote_literal('Week')||',' ||quote_literal(to_char(pPeriodStart,'yyyy-mm-dd'))||',' ||quote_literal(to_char(pPeriodEnd,'yyyy-mm-dd'))||',' ||' '||pRefTableRowCount||' )';
		      EXECUTE IMMEDIATE sqlstmt;

		      pRefTableRowCount := pRefTableRowCount + ROW_COUNT;

	          SELECT DATE(MIN(PERIOD_START_DATE_TIME)), DATE(MAX(PERIOD_END_DATE_TIME))
		  		INTO pPeriodStart,pPeriodEnd
		  		FROM (
					SELECT DENSE_RANK() OVER(ORDER BY PERIOD_YEAR DESC ) Y, PERIOD_YEAR, PERIOD_TYPE, PERIOD_NUMBER, PERIOD_START_DATE_TIME, PERIOD_END_DATE_TIME
				  	FROM MHO_ASIA_BI_DEV.ADS.VIEW_DIM_DATE
					WHERE PERIOD_END_DATE_TIME <= current_date  AND PERIOD_START_DATE_TIME >= pPeriodEnd AND PERIOD_TYPE = 'D' 
				) T

		      sqlstmt := 'insert into '||REFTABLENAME||' select REP.SP_PSYIELD_CUMULATIVE_INTERNAL(' ||quote_literal(pSiteCode)||',' ||quote_literal(pYieldGroup)||',' ||quote_literal(pYieldType)||',' ||quote_literal(pProgramList)||',' ||quote_literal('Day')||',' ||quote_literal(to_char(pPeriodStart,'yyyy-mm-dd'))||',' ||quote_literal(to_char(pPeriodEnd,'yyyy-mm-dd'))||',' ||' '||pRefTableRowCount||' )';
		      EXECUTE IMMEDIATE sqlstmt;

		    end if;
		    RETURN REFTABLE;
		
		END;
	END;
END_PROC;

