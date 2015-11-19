CREATE OR REPLACE PROCEDURE REP.SP_TMP_TABLE_REPLACE(CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY), CHARACTER VARYING(ANY))
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
	DECLARE
	
	pTableTmpName ALIAS FOR $1;
	pTableNewName ALIAS FOR $2;
	pTableTmpSchema ALIAS FOR $3;
	pTableNewSchema ALIAS FOR $4;
	
	pTmpExistFlag boolean;
	pNewExistFlag boolean;
	
	BEGIN
		BEGIN AUTOCOMMIT ON
			
			SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pTmpExistFlag FROM _V_TABLE WHERE TABLENAME = pTableTmpName AND SCHEMA = pTableTmpSchema;
			SELECT CASE WHEN COUNT(1) >=1 THEN TRUE ELSE FALSE END INTO pNewExistFlag FROM _V_TABLE WHERE TABLENAME = pTableNewName AND SCHEMA = pTableNewSchema;
			
			IF pTmpExistFlag = TRUE  AND pNewExistFlag = TRUE THEN
				
				EXECUTE IMMEDIATE 'DROP TABLE ' || pTableNewSchema || '.'||pTableNewName;
				RAISE NOTICE 'new table %.% exist, drop it.', pTableNewSchema, pTableNewName;
			
				EXECUTE IMMEDIATE 'ALTER TABLE ' || pTableTmpSchema || '.'||pTableTmpName || ' RENAME TO ' ||pTableNewSchema || '.'||pTableNewName ;
				RAISE NOTICE 'rename tmp table %.% to new table %.%', pTableTmpSchema, pTableTmpName, pTableNewSchema, pTableNewName;
				
			ELSIF pTmpExistFlag = TRUE THEN 
				EXECUTE IMMEDIATE 'ALTER TABLE ' || pTableTmpSchema || '.'||pTableTmpName || ' RENAME TO ' ||pTableNewSchema || '.'||pTableNewName ;
				RAISE NOTICE 'rename tmp table %.% to new table %.%', pTableTmpSchema, pTableTmpName, pTableNewSchema, pTableNewName;
			
			ELSIF pNewExistFlag = TRUE THEN 
				RAISE EXCEPTION 'Only new table exist, error here';
				
			ELSE
				RAISE NOTICE 'no tmp table, or new table exist';
			END IF;
			
		END;

	END;
END_PROC;

