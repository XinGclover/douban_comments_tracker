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