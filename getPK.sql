 SELECT 
   tabschema, tabname, colname 
 FROM 
   syscat.columns 
 WHERE 
   keyseq IS NOT NULL AND 
   keyseq > 0 
 ORDER BY 
   tabschema, tabname, keyseq 