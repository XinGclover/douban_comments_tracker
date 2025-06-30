CREATE TABLE public.linjiangxian_heat_iqiyi (
	insert_time TIMESTAMP WITH TIME ZONE DEFAULT now(), 
	heat_info INTEGER,                 
    effect_score NUMERIC(3,1),           	                      
    hot_link INTEGER,                                        
	
	CONSTRAINT linjiangxian_heat_time UNIQUE (insert_time)
);

DROP TABLE public.linjiangxian_heat_iqiyi;


SHOW timezone;


SET TIMEZONE TO 'Asia/Shanghai';



ALTER TABLE zhaoxuelu_heat_iqiyi ADD COLUMN timestamp_shanghai TIMESTAMP;

--Convert to Shanghai time
UPDATE zhaoxuelu_heat_iqiyi
SET timestamp_shanghai = insert_time AT TIME ZONE 'Asia/Shanghai';
 