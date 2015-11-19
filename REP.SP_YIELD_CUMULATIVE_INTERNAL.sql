CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_CUMULATIVE_INTERNAL(
    CHARACTER VARYING(ANY),
    CHARACTER VARYING(ANY),
    CHARACTER VARYING(ANY),
    CHARACTER VARYING(ANY),
    CHARACTER VARYING(ANY),
    CHARACTER VARYING(ANY),
    CHARACTER VARYING(ANY),
    BIGINT
)
RETURNS REFTABLE(MHO_ASIA_BI_DEV.REP.SPTBL_PSYIELD_CUMULATIVE)
EXECUTE AS OWNER
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
			
			sqlCalendar := 'select
    			t.calendar_date_display_name
    			,'||quote_literal(pPeriodType) ||' calendar_period_type
    			,t.calendar_period_start_date
    			,t.calendar_period_end_date
    			,t.calendar_site_code
    			,t.calendar_fiscal_year
    			,t.calendar_fiscal_quarter
    			,t.calendar_work_week
    			,t.calendar_date
    			,t.calendar_period_key --to be used for joining with actual results
    			from (
    				select ' ||
                case
                when pPeriodType='Quarter' then '''Q''||nvl(VWc.fiscal_quarter,'''')||''''''''||substring(nvl(VWc.fiscal_year,''''),3) calendar_date_display_name
                							,VWc.fiscal_year||''Q''||VWc.fiscal_quarter calendar_period_key
                							,row_number() over (partition by VWg.names,VWc.fiscal_year,VWc.fiscal_quarter order by VWc.date asc) calendar_date_asc_order
                							,min(date) over (partition by VWg.names,VWc.fiscal_year,VWc.fiscal_quarter) calendar_period_start_date
                							,max(date) over (partition by VWg.names,VWc.fiscal_year,VWc.fiscal_quarter) calendar_period_end_date
                							' when pPeriodType='Week' then '''W''||nvl(VWc.work_week,0)||''Q''||nvl(VWc.fiscal_quarter,'''')||''''''''||substring(nvl(VWc.fiscal_year,''''),3) calendar_date_display_name
                							,VWc.fiscal_year||''W''||VWc.work_week calendar_period_key
                							,row_number() over (partition by VWg.names,VWc.fiscal_year,VWc.work_week order by VWc.date asc) calendar_date_asc_order
                							,min(date) over (partition by VWg.names,VWc.fiscal_year,VWc.work_week) calendar_period_start_date
                							,max(date) over (partition by VWg.names,VWc.fiscal_year,VWc.work_week) calendar_period_end_date
                							' when pPeriodType='Day' then 'to_char(VWc.date,''mm/dd/yyyy'') calendar_date_display_name
                							,VWc.fiscal_year||''D''||to_char(VWc.date,''yyyymmdd'') calendar_period_key
                							,row_number() over (partition by VWg.names,VWc.date order by null) calendar_date_asc_order
                							,min(date) over (partition by VWg.names,VWc.date) calendar_period_start_date
                							,max(date) over (partition by VWg.names,VWc.date) calendar_period_end_date
                							' else 'NULL' end || '
                          ,VWg.names calendar_site_code
                					,VWc.fiscal_year calendar_fiscal_year
                					,VWc.fiscal_quarter calendar_fiscal_quarter
                					,VWc.work_week calendar_work_week
                					,VWc.date calendar_date
                					from pub_calendar_vw VWc join pub_geographic_area_vw VWg on VWc.location_sk_id=VWg.location_sk_id
                					where VWc.date between to_date('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') and to_date('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')' ||
		        case
		        when length(pSiteCode) > 0 then ' and VWg.names in ('||quote_literal(pSiteCode)||')' else '' end || ') t where t.calendar_date_asc_order=1';

    		raise notice 'sqlCalendar=%',sqlCalendar;

    		sqlYieldTarget := 'select
    			site_code target_site_code
    			,level target_level
    			,fiscal_year target_fiscal_year
    			,fiscal_quarter target_fiscal_quarter
    			,case when NULL_YIELD_TARGET_COUNT=0 then yield_target else null end target_yield_target
    			from (
          			select *
          			,count(1) over (partition by site_code,level,fiscal_year,fiscal_quarter) YIELD_TARGET_PER_QUARTER_COUNT
          			,sum(case when yield_target is null then 1 else 0 end) over (partition by site_code) NULL_YIELD_TARGET_COUNT
          			from (
          			     select distinct p.SITE_CODE,t.level,t.fiscal_year,t.FISCAL_QUARTER,yield_target
                     from (
                      			select distinct p.site_code, p.product_group_02 from pub_head_product_vw p
                      			where p.site_code='||quote_literal(pSiteCode)||' ' ||
                          case
                          when length(pProgramList) > 0 then ' and p.program in ('|| pProgramList ||') ' else '' end || ') p ' || ' left outer join rptg_lu_yield_target t on p.site_code=t.site_code and p.product_group_02=t.product_group_02
                          			and t.level='||quote_literal(pYieldGroup) || '
                      			) j
    			) t where YIELD_TARGET_PER_QUARTER_COUNT=1 and fiscal_year is not null and fiscal_quarter is not null';

    		raise notice 'sqlYieldTarget=%',sqlYieldTarget;

    		sqlRulesOperation := 'select
		    			RULES_SITE_CODE
		    			,RULES_OPERATION_NAME
		    			,RULES_TX_LOT_HISTORY_SITE_CODE
		    			,RULES_RPTG_OPERATION_NAME
		    			,RULES_PRIMARY_RULE_NAME
		    			,RULES_02_RULE_NAME
		    			,RULES_DEFECT_RULE_NAME
		    			,RULES_PROGRAM_WITH_PN_SUFFIX_FLAG
		    			' ||
		          case
		              when pYieldGroup
		              like '4%' then ',RPTG_PROCESS_GROUP_ID RULES_RPTG_AGG_GROUP_ID,RPTG_PROCESS_GROUP RULES_RPTG_AGG_GROUP' when pYieldGroup
		              like '3%' then ',RPTG_PROCESS_TYPE_ID RULES_RPTG_AGG_GROUP_ID,RPTG_PROCESS_TYPE RULES_RPTG_AGG_GROUP' when pYieldGroup
		              like '2%' then ',RPTG_YIELD_GROUP_ID RULES_RPTG_AGG_GROUP_ID,RPTG_YIELD_GROUP RULES_RPTG_AGG_GROUP'
		              else ',RPTG_OVERALL_GROUP_ID RULES_RPTG_AGG_GROUP_ID,RPTG_OVERALL_GROUP RULES_RPTG_AGG_GROUP' end || ' from RPTG_LU_YIELD_RULES_OPERATION where RULES_SITE_CODE=' || quote_literal(pSiteCode) ||
		          case
		          when pYieldGroup
		          like ( '4%' ) then ' and RPTG_PROCESS_GROUP_ID like ' || quote_literal(pYieldGroup||'%')
		          when pYieldGroup
		          like ( '3%' ) then ' and RPTG_PROCESS_TYPE_ID like ' || quote_literal(pYieldGroup||'%')
		          when pYieldGroup
		          like ( '2%' ) then ' and RPTG_YIELD_GROUP_ID like ' || quote_literal(pYieldGroup||'%') else ' ' end;

    		raise notice 'sqlRulesOperation=%',sqlRulesOperation;

		    if pPeriodType='Quarter' then pPeriodFields := ',m_lh_operation_year,m_lh_operation_quarter';
		                                  pPeriodFieldKey := ',m_lh_operation_year||''Q''||m_lh_operation_quarter ';
		    elsif pPeriodType='Week' then pPeriodFields := ',m_lh_operation_year,m_lh_operation_week';
		                                  pPeriodFieldKey := ',m_lh_operation_year||''W''||m_lh_operation_week ';
		    elsif pPeriodType='Day' then  pPeriodFields := ',m_lh_operation_year,m_lh_operation_date';
		                                  pPeriodFieldKey := ',m_lh_operation_year||''D''||to_char(m_lh_operation_date,''yyyymmdd'') ';
		    else  pPeriodFields := '';
		          pPeriodFieldKey := '';
		    end if;

   			sqlDefectLog := 'select
		    			t.M_LH_SITE_CODE defect_site_code
		    			'||pPeriodFieldKey||' defect_period_key
		    			,o.RULES_RPTG_OPERATION_NAME defect_RPTG_OPERATION_NAME
		    			,sum(t.sum_defect * r.defect_factor ) sum_defect
		    			from RPTG_MITECS_TX_DEFECT_LOG_AGG_VW t
		    			join RPTG_LU_YIELD_RULES_DEFECT r on t.m_lh_old_operation_sk_id_operation_name = r.defect_operation_name and t.m_lh_site_code = r.defect_tx_lot_history_site_code	and t.DEFECT_CODE = r.defect_code
		    			join RPTG_LU_YIELD_RULES_OPERATION o on o.rules_site_code='||quote_literal(pSiteCode)||' and r.DEFECT_RULE_NAME=o.RULES_DEFECT_RULE_NAME
		    			where
		        			--same condition as below!
		        			t.IS_TEST_LOT_ID_FLAG=false
		        			and t.M_LH_DELETED_FLAG=false
		        			and t.M_LH_GROUP_FLAG=false
		        			and t.M_LH_TRANSACTION_NAME in (select rules_transaction_name from RPTG_LU_YIELD_RULES_TRANSACTION_NAME) ' ||
		          case
		          when length(pProgramList) > 0 then ' and t.M_LH_OLD_PRODUCT_SK_ID_PROGRAM in ('|| pProgramList ||') ' else '' end || '
		          			and ( o.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG is null or o.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG = t.M_LH_OLD_PRODUCT_SK_ID_PROGRAM_WITH_PN_SUFFIX_FLAG )
		          			--date range
		          			and t.M_LH_OPERATION_DATE between to_date('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') and to_date('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')
		          			--owner and facility location
		          			and (r.DEFECT_OWNER_FACILITY_RULE_NAME,t.M_LH_OWNER) in (select distinct OWNER_RULE_NAME,OWNER_VALUE from rptg_lu_yield_rules_owner where OWNER_YIELD_TYPE='||quote_literal(pYieldType)||' )
		          			and (r.DEFECT_OWNER_FACILITY_RULE_NAME,t.M_LH_SITE_CODE, t.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION) in (select distinct FACILITY_RULE_NAME,FACILITY_TX_LOT_HISTORY_SITE_CODE,facility_location_value from RPTG_LU_YIELD_RULES_FACILITY_LOCATION  )
		          			group by t.M_LH_SITE_CODE '||pPeriodFields||'
		          			,o.RULES_RPTG_OPERATION_NAME';

    		raise notice 'sqlDefectLog=%',sqlDefectLog;

			sqlReworkLoss := 'select
						rl.M_LH_SITE_CODE reworkloss_site_code
						'||pPeriodFieldKey||' reworkloss_period_key
						,rules.RULES_RPTG_OPERATION_NAME reworkloss_RPTG_OPERATION_NAME
						,sum(rl.sum_loss) sum_reworkloss
						from RPTG_MITECS_TX_LOT_HISTORY_AGG_VW rl
							join ('|| sqlRulesOperation ||' ) rules
								on rl.M_LH_OLD_ROUTE_SK_ID_ROUTE_NAME_REWORK_SUFFIX is not null and rl.m_lh_site_code = rules.RULES_TX_LOT_HISTORY_SITE_CODE and rl.M_LH_OLD_ROUTE_SK_ID_ROUTE_NAME_REWORK_SUFFIX = rules.rules_operation_name and length(rules.RULES_RPTG_AGG_GROUP_ID) > 0
						where
						--same condition as below!
			      			rl.IS_TEST_LOT_ID_FLAG=false
			      			and rl.M_LH_DELETED_FLAG=false
			      			and rl.M_LH_GROUP_FLAG=false
			      			and rl.M_LH_TRANSACTION_NAME in (select rules_transaction_name from RPTG_LU_YIELD_RULES_TRANSACTION_NAME) ' ||
			      case
			      when length(pProgramList) > 0 then ' and rl.M_LH_OLD_PRODUCT_SK_ID_PROGRAM in ('|| pProgramList ||') ' else '' end || '
			      			and ( rules.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG is null or rules.RULES_PROGRAM_WITH_PN_SUFFIX_FLAG = rl.M_LH_OLD_PRODUCT_SK_ID_PROGRAM_WITH_PN_SUFFIX_FLAG )

			      			--date range
			      			and rl.M_LH_OPERATION_DATE between to_date('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') and to_date('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')
			      			--owner and facility location
			      			and
			      			(
			      				-- RULES_PRIMARY_RULE_NAME
			      				(
			      					(rules.RULES_PRIMARY_RULE_NAME, rl.M_LH_OWNER) in (select distinct OWNER_RULE_NAME,OWNER_VALUE from rptg_lu_yield_rules_owner where OWNER_DENOMINATOR_COUNT_REWORK_LOSS=1 and OWNER_YIELD_TYPE='||quote_literal(pYieldType)||' )
			      					and (rules.RULES_PRIMARY_RULE_NAME, rl.M_LH_SITE_CODE, rl.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION) in (select distinct FACILITY_RULE_NAME,FACILITY_TX_LOT_HISTORY_SITE_CODE,facility_location_value from RPTG_LU_YIELD_RULES_FACILITY_LOCATION where FACILITY_DENOMINATOR_COUNT_REWORK_LOSS=1 )
			      				)
			      				or
			      				-- RULES_02_RULE_NAME
			      				(
			      					(rules.RULES_02_RULE_NAME, rl.M_LH_OWNER) in (select distinct OWNER_RULE_NAME,OWNER_VALUE from rptg_lu_yield_rules_owner where OWNER_DENOMINATOR_COUNT_REWORK_LOSS=1 and OWNER_YIELD_TYPE='||quote_literal(pYieldType)||' )
			      					and (rules.RULES_02_RULE_NAME, rl.M_LH_SITE_CODE, rl.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION) in (select distinct FACILITY_RULE_NAME,FACILITY_TX_LOT_HISTORY_SITE_CODE,facility_location_value from RPTG_LU_YIELD_RULES_FACILITY_LOCATION where FACILITY_DENOMINATOR_COUNT_REWORK_LOSS=1 )
			      				)
			      			)
			      			group by rl.M_LH_SITE_CODE '||pPeriodFields||'
			      			,rules.RULES_RPTG_OPERATION_NAME';

			raise notice 'sqlReworkLoss=%',sqlReworkLoss;

    		sqlYieldActual := 'select
    			--m_lh_site_code actual_site_code
    			RULES_SITE_CODE actual_site_code
    			,RULES_RPTG_AGG_GROUP_ID actual_yield_group
    			,actual_period_key
    			,exp(sum(ln(rptg_operation_yield))) actual_yield_actual
    			,case when length(min(ACTUAL_COMMENTS)) > 0 then true else false end actual_has_comments_flag
    			,min(ACTUAL_COMMENTS) actual_comments
    			from ( --Begins ACTUAL_YIELD_STEP_04_ZERO_DENOMINATOR_AND_NUMERATOR_HANDLING_WITHOUT_AGGREGATION
          				select
          				m_lh_site_code
          				,RULES_SITE_CODE
          				,RULES_RPTG_AGG_GROUP_ID
          				,RULES_RPTG_OPERATION_NAME
          				,actual_period_key
          				,case when denominator_per_operation <= 0 or numerator_per_operation <= 0 then 1.0 else cast(numerator_per_operation as double)/denominator_per_operation end rptg_operation_yield
          				,case when numerator_per_operation <= 0 then ''Operation ''|| RULES_RPTG_OPERATION_NAME || ' || quote_literal(' has invalid total OUT amount=') ||'||numerator_per_operation||'||quote_literal('. ')||' else null end ACTUAL_COMMENTS
          				from
        					(--Begins ACTUAL_YIELD_STEP_03_DEFECT_AND_REWORK_ROUTE_HANDLING
            						select
            						m_lh_site_code
            						,actual_period_key
            						,RULES_SITE_CODE
            						,RULES_RPTG_AGG_GROUP_ID
            						,RULES_RPTG_OPERATION_NAME
            						--STEP_03 aggregated fields
            						,nvl(numerator_per_operation,0) numerator_per_operation
            						,(
            							nvl(denominator_per_operation,0)
            							+ nvl(sum_defect,0)
            							+ nvl(sum_reworkloss,0)
            						) denominator_per_operation
            						--ends STEP_03 aggregated fields
            						from
            						(--Begins ACTUAL_YIELD_STEP_02_RULE_EXECUTION_ON_OWNER_LOCATION_ON_MAIN_OPERATION_NAME
              							select
              							m_lh_site_code
              							,actual_period_key
              							,RULES_SITE_CODE
              							,RULES_RPTG_AGG_GROUP_ID
              							,RULES_RPTG_OPERATION_NAME
              							--STEP_02 aggregated fields
              							,(
              								nvl(sum(sum_out * rules01o.OWNER_NUMERATOR_COUNT_OUT * rules01f.FACILITY_NUMERATOR_COUNT_OUT ),0)

              								+ nvl(sum(sum_out * rules02o.OWNER_NUMERATOR_COUNT_OUT * rules02f.FACILITY_NUMERATOR_COUNT_OUT ),0)
              							) numerator_per_operation
              							,(
              								nvl(sum(sum_out * rules01o.OWNER_DENOMINATOR_COUNT_OUT * rules01f.FACILITY_DENOMINATOR_COUNT_OUT ),0)
              								+ nvl(sum(sum_loss * rules01o.OWNER_DENOMINATOR_COUNT_LOSS * rules01f.FACILITY_DENOMINATOR_COUNT_LOSS),0)

              								+ nvl(sum(sum_out * rules02o.OWNER_DENOMINATOR_COUNT_OUT * rules02f.FACILITY_DENOMINATOR_COUNT_OUT ),0)
              								+ nvl(sum(sum_loss * rules02o.OWNER_DENOMINATOR_COUNT_LOSS * rules02f.FACILITY_DENOMINATOR_COUNT_LOSS),0)
              							) denominator_per_operation
              							--ends STEP_02 aggregated fields
              							from
              							( --Begins ACTUAL_YIELD_STEP_01_DATA_SELECTION_AND_AGGREGATION_PRESERVING_OWNER_AND_FACILITY_LOCATION
                  								select
                  								lh.m_lh_site_code
                  								'||pPeriodFieldKey||' actual_period_key
                  								,lh.M_LH_OWNER
                  								,lh.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION
                  								--all rule fields have to go in here as well as the GROUP BY below
                  								,RULES_SITE_CODE
                  								,RULES_RPTG_AGG_GROUP_ID
                  								,RULES_RPTG_OPERATION_NAME
                  								,RULES_PRIMARY_RULE_NAME
                  								,RULES_02_RULE_NAME
                  								--ends rule fields
                  								--STEP_01 aggregated fields (still preserving the owner and facility_location)
                  								,sum(lh.sum_in) sum_in
                  								,sum(lh.sum_out) sum_out
                  								,sum(lh.sum_loss) sum_loss
                  								--ends STEP_01 aggregated fields
                  								from RPTG_MITECS_TX_LOT_HISTORY_AGG_VW lh
                  								join ('|| sqlRulesOperation ||') rules
                  									on lh.m_lh_site_code=rules.RULES_TX_LOT_HISTORY_SITE_CODE and lh.m_lh_old_operation_sk_id_operation_name=rules.rules_operation_name and length(rules.RULES_RPTG_AGG_GROUP_ID) > 0
                  								where
                  								lh.IS_TEST_LOT_ID_FLAG=false
                  								and lh.M_LH_DELETED_FLAG=false
                  								and lh.M_LH_GROUP_FLAG=false
                  								and lh.M_LH_TRANSACTION_NAME in (select rules_transaction_name from RPTG_LU_YIELD_RULES_TRANSACTION_NAME) ' ||
                              case
                              when length(pProgramList) > 0 then ' and lh.M_LH_OLD_PRODUCT_SK_ID_PROGRAM in ('|| pProgramList ||') ' else '' end || '
                  								--the M_LH_OLD_PRODUCT_SK_ID_PROGRAM_WITH_PN_SUFFIX_FLAG rule
                  								and ( RULES_PROGRAM_WITH_PN_SUFFIX_FLAG is null or RULES_PROGRAM_WITH_PN_SUFFIX_FLAG=lh.M_LH_OLD_PRODUCT_SK_ID_PROGRAM_WITH_PN_SUFFIX_FLAG )
                  								--date range
                  								and lh.M_LH_OPERATION_DATE between to_date('||quote_literal(pPeriodStart)||','||quote_literal('yyyy-mm-dd')||') and to_date('||quote_literal(pPeriodEnd)||','||quote_literal('yyyy-mm-dd')||')

                  								--the following two clauses are for 1st-round filtering.  additional rules are in step 02
                  								and
                  								(
                  									(
                  										(RULES_PRIMARY_RULE_NAME,lh.M_LH_OWNER) in (select distinct OWNER_RULE_NAME,OWNER_VALUE from rptg_lu_yield_rules_owner where OWNER_YIELD_TYPE='||quote_literal(pYieldType)||' )
                  										and (RULES_PRIMARY_RULE_NAME,lh.m_lh_site_code,lh.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION) in (select distinct FACILITY_RULE_NAME,FACILITY_TX_LOT_HISTORY_SITE_CODE,facility_location_value from RPTG_LU_YIELD_RULES_FACILITY_LOCATION  )
                  									)
                  									or
                  									(
                  										(RULES_02_RULE_NAME,lh.M_LH_OWNER) in (select distinct OWNER_RULE_NAME,OWNER_VALUE from rptg_lu_yield_rules_owner where OWNER_YIELD_TYPE='||quote_literal(pYieldType)||' )
                  										and (RULES_02_RULE_NAME,lh.m_lh_site_code,lh.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION) in (select distinct FACILITY_RULE_NAME,FACILITY_TX_LOT_HISTORY_SITE_CODE,facility_location_value from RPTG_LU_YIELD_RULES_FACILITY_LOCATION  )
                  									)
                  								)
                  								group by lh.m_lh_site_code '||pPeriodFields||',lh.M_LH_OWNER,lh.M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION

                  								--all rule fields have to go in here as well as in the SELECT above
                  									,RULES_SITE_CODE
                  									,RULES_RPTG_AGG_GROUP_ID
                  									,RULES_RPTG_OPERATION_NAME
                  									,RULES_PRIMARY_RULE_NAME
                  									,RULES_02_RULE_NAME
                  								--ends rule fields
                  					) ACTUAL_YIELD_STEP_01_DATA_SELECTION_AND_AGGREGATION_PRESERVING_OWNER_AND_FACILITY_LOCATION
              							left outer join rptg_lu_yield_rules_owner rules01o
              								on RULES_PRIMARY_RULE_NAME=rules01o.OWNER_RULE_NAME and M_LH_OWNER=rules01o.OWNER_VALUE and rules01o.OWNER_YIELD_TYPE='||quote_literal(pYieldType)||'
              							left outer join RPTG_LU_YIELD_RULES_FACILITY_LOCATION rules01f
              								on RULES_PRIMARY_RULE_NAME=rules01f.FACILITY_RULE_NAME and M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION=rules01f.facility_location_value and m_lh_site_code=rules01f.FACILITY_TX_LOT_HISTORY_SITE_CODE
              							left outer join rptg_lu_yield_rules_owner rules02o
              								on RULES_02_RULE_NAME=rules02o.OWNER_RULE_NAME and M_LH_OWNER=rules02o.OWNER_VALUE and rules02o.OWNER_YIELD_TYPE='||quote_literal(pYieldType)||'
              							left outer join RPTG_LU_YIELD_RULES_FACILITY_LOCATION rules02f
              								on RULES_02_RULE_NAME=rules02f.FACILITY_RULE_NAME and M_LH_OLD_FACILITY_SK_ID_FACILITY_LOCATION=rules02f.facility_location_value and m_lh_site_code=rules02f.FACILITY_TX_LOT_HISTORY_SITE_CODE
              							group by m_lh_site_code,actual_period_key,RULES_SITE_CODE,RULES_RPTG_AGG_GROUP_ID,RULES_RPTG_OPERATION_NAME
    						      ) ACTUAL_YIELD_STEP_02_RULE_EXECUTION_ON_OWNER_LOCATION_ON_MAIN_OPERATION_NAME
          						left outer join ('|| sqlDefectLog ||') dl
          							on M_LH_SITE_CODE=defect_SITE_CODE
          								and actual_period_key=defect_period_key
          								and RULES_RPTG_OPERATION_NAME=defect_RPTG_OPERATION_NAME
          						left outer join ('|| sqlReworkLoss || ') rl
          							on M_LH_SITE_CODE=reworkloss_SITE_CODE
          								and actual_period_key=reworkloss_period_key
          								and RULES_RPTG_OPERATION_NAME=reworkloss_RPTG_OPERATION_NAME
    					    ) ACTUAL_YIELD_STEP_03_DEFECT_AND_REWORK_ROUTE_HANDLING
    			    ) ACTUAL_YIELD_STEP_04_ZERO_DENOMINATOR_AND_NUMERATOR_HANDLING_WITHOUT_AGGREGATION
    			-- and here is the final grouping for multiplication for actual yield
    			--group by m_lh_site_code,RULES_RPTG_AGG_GROUP_ID,actual_period_key
    			   group by RULES_SITE_CODE,RULES_RPTG_AGG_GROUP_ID,actual_period_key';

			raise notice 'sqlYieldActual=%',sqlYieldActual;

			sqlFinal := 'insert into '||REFTABLENAME||' select
					(row_number() over (order by calendar_site_code,calendar_date,yield_group_rptg_agg_group_id) + '||pRowNumberOffset||') calendar_row_number
					,calendar_date_display_name
					,calendar_period_type
					,calendar_period_start_date
					,calendar_period_end_date
					,actual_yield_actual
					,target_yield_target
					,calendar_site_code
					,yield_group_rptg_agg_group_id
					,'||quote_literal(pYieldType)||'
					,nvl(actual_has_comments_flag,false)
					, actual_comments
					from ('||sqlCalendar||') final_join_calendar
					join (select distinct rules_site_code yield_group_site_code, rules_rptg_agg_group_id yield_group_rptg_agg_group_id  from ('||sqlRulesOperation||') r where length(RULES_RPTG_AGG_GROUP_ID) > 0 ) final_join_yield_group on calendar_site_code=yield_group_site_code
					left outer join ( '||sqlYieldTarget||' ) final_join_target on calendar_site_code=target_site_code and calendar_fiscal_year=target_fiscal_year and calendar_fiscal_quarter=target_fiscal_quarter
					left outer join ( '||sqlYieldActual||' ) final_join_actual
						on calendar_site_code=actual_site_code
						and yield_group_rptg_agg_group_id=actual_yield_group
						and calendar_period_key=actual_period_key';

			raise notice 'sqlFinal=%',sqlFinal;
			
			EXECUTE IMMEDIATE sqlFinal;
			
			RETURN REFTABLE;
		
		END;
	END;
END_PROC;
