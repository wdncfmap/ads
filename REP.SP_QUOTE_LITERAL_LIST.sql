CREATE OR REPLACE PROCEDURE REP.SP_QUOTE_LITERAL_LIST(CHARACTER VARYING(ANY), CHARACTER VARYING(ANY))
RETURNS CHARACTER VARYING(ANY)
LANGUAGE NZPLSQL AS
BEGIN_PROC

	DECLARE 
		pInput ALIAS FOR $1;
		pSplitChar ALIAS FOR $2;
		pTemp varchar;
		pOutput varchar;
		pCurrentPosition int;
		pSplitCharPosition int;
		
	BEGIN
		BEGIN AUTOCOMMIT ON 
		
			if length(nvl(pSplitChar,''))=0 then 
				raise exception 'pSplitCharPosition cannot be blank';
			end if;

			pOutput := '';
			pTemp := pInput;

			pSplitCharPosition := position(pSplitChar in pTemp);
			while pSplitCharPosition > 0 
				loop 
					pOutput := pOutput || quote_literal(trim(substring(pTemp,1,pSplitCharPosition-1))) || pSplitChar;
					pTemp := substring(pTemp,pSplitCharPosition+length(pSplitChar));
					pSplitCharPosition := position(pSplitChar in pTemp);
				end loop;

			pOutput := pOutput || quote_literal(trim(pTemp));

			RETURN pOutput;
		
		END;
		
	END;
END_PROC;

