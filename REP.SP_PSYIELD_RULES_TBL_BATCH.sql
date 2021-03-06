CREATE OR REPLACE PROCEDURE REP.SP_PSYIELD_RULES_TBL_BATCH()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
 BEGIN

DROP TABLE  REP.PSYIELD_OPERATION_SETUP
CREATE TABLE REP.PSYIELD_OPERATION_SETUP ( SUBJECT_AREA CHARACTER(4)
NOT NULL, SITE_CODE CHARACTER(3)
NOT NULL, OPERATION_NAME CHARACTER VARYING(32)
NOT NULL, OPERATION_DESCRIPTION CHARACTER VARYING(32)
NOT NULL, PROCESS_TOOL_PREFIX CHARACTER VARYING(5), TAG_NAME_RECIPE CHARACTER VARYING(50), TAG_NAME_RUN_NUMBER CHARACTER VARYING(50), TAG_NAME_START_TIME CHARACTER VARYING(50), TAG_NAME_END_TIME CHARACTER VARYING(50), DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (SUBJECT_AREA);

ALTER TABLE REP.PSYIELD_OPERATION_SETUP
ADD CONSTRAINT PK_PSYIELD_OPERATION_SETUP
PRIMARY KEY (SUBJECT_AREA, SITE_CODE, OPERATION_NAME);

COMMENT ON TABLE PSYIELD_OPERATION_SETUP
IS 'Definition of an operation setup for reports ';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.SUBJECT_AREA
IS 'Subject area like SPC, LAP, BQST, DET, etc.,';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.SITE_CODE
IS 'Site code like WDF, WDB, PNS, WDN, etc.,';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.OPERATION_NAME
IS 'Operation name, generally 4 digit number for WDB and 5 digit numbber for PNS';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.OPERATION_DESCRIPTION
IS 'Operation description to be visible in report';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.PROCESS_TOOL_PREFIX
IS 'SPC tool name prefix to be searched upon in SPC TagValue table';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.TAG_NAME_RECIPE
IS 'SPC tool name prefix to be searched upon in SPC TagValue table';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.TAG_NAME_RUN_NUMBER
IS 'SPC run number to be searched upon in SPC TagValue table';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.TAG_NAME_START_TIME
IS 'SPC process start date and time prefix to be searched upon in SPC TagValue table';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.TAG_NAME_END_TIME
IS 'SPC process end date and time to be searched upon in SPC TagValue table';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN PSYIELD_OPERATION_SETUP.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

DROP TABLE REP.PSYIELD_RULE_YIELD_DEFECT
CREATE TABLE REP.PSYIELD_RULE_YIELD_DEFECT ( RULE_NAME CHARACTER VARYING(50)
NOT NULL, SITE_CODE CHARACTER(3)
NOT NULL, OWNER_FACILITY_RULE_NAME CHARACTER VARYING(50)
NOT NULL, OPERATION_NAME CHARACTER VARYING(32)
NOT NULL, DEFECT_KEY BIGINT
NOT NULL, DEFECT_CODE CHARACTER VARYING(32)
NOT NULL, DEFECT_FACTOR INTEGER
NOT NULL, DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (RULE_NAME);

ALTER TABLE REP.PSYIELD_RULE_YIELD_DEFECT
ADD CONSTRAINT PK_PSYIELD_RULE_YIELD_DEFECT
PRIMARY KEY (RULE_NAME, OWNER_FACILITY_RULE_NAME, OPERATION_NAME, DEFECT_CODE);

COMMENT ON TABLE PSYIELD_RULE_YIELD_DEFECT
IS 'Definition of an operation/defect code rule setup for reports ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.RULE_NAME
IS 'Name of rule';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.SITE_CODE
IS 'Site code like WDF, WDB, PNS, WDN, etc.,';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.OWNER_FACILITY_RULE_NAME
IS 'Owner facility rule name';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.OPERATION_NAME
IS 'Operation name, generally 4 digit number for WDB and 5 digit numbber for PNS';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.DEFECT_KEY
IS 'Unique auto-incrementing integer internally generated by database for each defect';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.DEFECT_CODE
IS 'Defect code';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.DEFECT_FACTOR
IS 'Factor to be used in NZPLSQL procedure calculations';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_DEFECT.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

DROP TABLE REP.PSYIELD_RULE_YIELD_TARGET
CREATE TABLE REP.PSYIELD_RULE_YIELD_TARGET ( SITE_CODE CHARACTER VARYING(3)
NOT NULL,
LEVEL INTEGER
NOT NULL, PRODUCT_GROUP_02 CHARACTER VARYING(32)
NOT NULL, FISCAL_YEAR INTEGER
NOT NULL, FISCAL_QUARTER INTEGER
NOT NULL, YIELD_TARGET NUMERIC(10,9)
NOT NULL, DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (SITE_CODE);

ALTER TABLE REP.PSYIELD_RULE_YIELD_TARGET
ADD CONSTRAINT PK_PSYIELD_RULE_YIELD_TARGET
PRIMARY KEY (SITE_CODE, PRODUCT_GROUP_02);

COMMENT ON TABLE PSYIELD_RULE_YIELD_TARGET
IS 'Definition of a yield target for a product group ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.SITE_CODE
IS 'Site code like WDB, PNS, WDN, etc., ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.
LEVEL IS 'Level in report';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.PRODUCT_GROUP_02
IS 'Product group 02';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.FISCAL_YEAR
IS 'Fiscal year';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.FISCAL_QUARTER
IS 'Fiscal quarter';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.YIELD_TARGET
IS 'Yield target for product group, This is expressed in terms of number (100% yield means 1.00)';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TARGET.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

DROP TABLE REP.PSYIELD_RULE_YIELD_OPERATION
CREATE TABLE REP.PSYIELD_RULE_YIELD_OPERATION ( RULE_NAME CHARACTER VARYING(50)
NOT NULL, RULE_NAME_02 CHARACTER VARYING(50), SITE_CODE CHARACTER(3)
NOT NULL, OPERATION_NAME CHARACTER VARYING(32)
NOT NULL, LOT_HISTORY_SITE_CODE CHARACTER(3)
NOT NULL, REPORT_OPERATION_NAME CHARACTER VARYING(32)
NOT NULL, RULE_NAME_DEFECT CHARACTER VARYING(50), INCLUDE_PN_SUFFIX_FLAG CHARACTER(1), PROCESS_TYPE_ID CHARACTER VARYING(32), PROCESS_TYPE CHARACTER VARYING(100), PROCESS_GROUP_ID CHARACTER VARYING(32), PROCESS_GROUP CHARACTER VARYING(100), YIELD_GROUP_ID CHARACTER VARYING(32), YIELD_GROUP CHARACTER VARYING(100), OVERALL_GROUP_ID CHARACTER VARYING(32), OVERALL_GROUP CHARACTER VARYING(100), DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (RULE_NAME);

ALTER TABLE REP.PSYIELD_RULE_YIELD_OPERATION
ADD CONSTRAINT PK_PSYIELD_RULE_YIELD_OPERATION
PRIMARY KEY (RULE_NAME, OPERATION_NAME);

COMMENT ON TABLE PSYIELD_RULE_YIELD_OPERATION
IS 'Definition of an operation rule setup for yield reports ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.RULE_NAME
IS 'Name of rule';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.RULE_NAME_02
IS 'Name of rule2';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.SITE_CODE
IS 'Site code like WDF, WDB, PNS, WDN, etc.,';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.OPERATION_NAME
IS 'Operation Name';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.LOT_HISTORY_SITE_CODE
IS 'Site code like WDF, WDB, PNS, WDN, etc.,';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.REPORT_OPERATION_NAME
IS 'Operation Name';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.RULE_NAME_DEFECT
IS 'Name of defect rule name';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.INCLUDE_PN_SUFFIX_FLAG
IS 'Y-Include program name with PN suffixes, N- do not include';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.PROCESS_TYPE_ID
IS 'Process Type ID';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.PROCESS_TYPE
IS 'Process Type';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.PROCESS_GROUP_ID
IS 'Process GROUP ID';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.PROCESS_GROUP
IS 'Process GROUP';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.YIELD_GROUP_ID
IS 'Yield GROUP ID';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.YIELD_GROUP
IS 'Yield GROUP';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.OVERALL_GROUP_ID
IS 'Overall GROUP ID';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.OVERALL_GROUP
IS 'Overall GROUP';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_OPERATION.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

DROP TABLE REP.PSYIELD_RULE_YIELD_OWNER
CREATE TABLE REP.PSYIELD_RULE_YIELD_OWNER ( RULE_NAME CHARACTER VARYING(50)
NOT NULL, YIELD_TYPE CHARACTER(10)
NOT NULL,
OWNER CHARACTER VARYING(32)
NOT NULL, NUMERATOR_COUNT_OUT INTEGER
NOT NULL, DENOMINATOR_COUNT_OUT INTEGER
NOT NULL, DENOMINATOR_COUNT_LOSS INTEGER
NOT NULL, DENOMINATOR_COUNT_REWORK INTEGER
NOT NULL, DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (RULE_NAME);

ALTER TABLE REP.PSYIELD_RULE_YIELD_OWNER
ADD CONSTRAINT PK_REP.PSYIELD_RULE_YIELD_OWNER
PRIMARY KEY (RULE_NAME);

COMMENT ON TABLE REP.PSYIELD_RULE_YIELD_OWNER
IS 'Definition of an OWNER rule setup for reports ';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.RULE_NAME
IS 'Name of rule';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.YIELD_TYPE
IS 'Prime, Blended, Rework, etc.,';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.
OWNER IS 'OWNER name';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.NUMERATOR_COUNT_OUT
IS 'Numerator out quantity ';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.DENOMINATOR_COUNT_OUT
IS 'Denominator out quantity';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.DENOMINATOR_COUNT_LOSS
IS 'Denominator out quantity for loss';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.DENOMINATOR_COUNT_REWORK
IS 'Denominator out quantity for rework';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN REP.PSYIELD_RULE_YIELD_OWNER.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

DROP TABLE REP.PSYIELD_RULE_YIELD_FACILITY
CREATE TABLE REP.PSYIELD_RULE_YIELD_FACILITY ( RULE_NAME CHARACTER VARYING(50)
NOT NULL, SITE_CODE CHARACTER(3)
NOT NULL, FACILITY CHARACTER VARYING(32)
NOT NULL, NUMERATOR_COUNT_OUT INTEGER
NOT NULL, DENOMINATOR_COUNT_OUT INTEGER
NOT NULL, DENOMINATOR_COUNT_LOSS INTEGER
NOT NULL, DENOMINATOR_COUNT_REWORK INTEGER
NOT NULL, DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (RULE_NAME);

ALTER TABLE REP.PSYIELD_RULE_YIELD_FACILITY
ADD CONSTRAINT PK_PSYIELD_RULE_YIELD_FACILITY
PRIMARY KEY (RULE_NAME);

COMMENT ON TABLE PSYIELD_RULE_YIELD_FACILITY
IS 'Definition of an facility rule setup for reports ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.RULE_NAME
IS 'Name of rule';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.SITE_CODE
IS 'Site code like WDF, WDB, PNS, WDN, etc.,';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.FACILITY
IS 'Facility name';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.NUMERATOR_COUNT_OUT
IS 'Numerator out quantity ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.DENOMINATOR_COUNT_OUT
IS 'Denominator out quantity';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.DENOMINATOR_COUNT_LOSS
IS 'Denominator out quantity for loss';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.DENOMINATOR_COUNT_REWORK
IS 'Denominator out quantity for rework';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_FACILITY.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

DROP TABLE REP.PSYIELD_RULE_YIELD_TRANS
CREATE TABLE REP.PSYIELD_RULE_YIELD_TRANS ( RULE_NAME CHARACTER VARYING(50)
NOT NULL, DELETED_FLAG CHARACTER(1)
NOT NULL DEFAULT 'N'::"NVARCHAR", SITE_KEY BIGINT
NOT NULL, LAST_UPDATE_DATE_TIME TIMESTAMP
NOT NULL DEFAULT "TIMESTAMP"('now(0)'::"VARCHAR") )
DISTRIBUTE ON (RULE_NAME);

ALTER TABLE REP.PSYIELD_RULE_YIELD_TRANS
ADD CONSTRAINT PK_PSYIELD_RULE_YIELD_TRANS
PRIMARY KEY (RULE_NAME);

COMMENT ON TABLE PSYIELD_RULE_YIELD_TRANS
IS 'Definition of a TRANS rule setup for reports ';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TRANS.RULE_NAME
IS 'Name of rule';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TRANS.DELETED_FLAG
IS 'Y-Means record marked for deletion. N - Means active record';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TRANS.SITE_KEY
IS 'Unique auto-incrementing integer internally generated by database for each site';

COMMENT ON COLUMN PSYIELD_RULE_YIELD_TRANS.LAST_UPDATE_DATE_TIME
IS 'Record created / last updated date and time';

END;

END_PROC;
