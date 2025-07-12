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
UPDATE shujuanyimeng_heat_iqiyi
SET timestamp_shanghai = insert_time AT TIME ZONE 'Asia/Shanghai'
WHERE timestamp_shanghai IS NULL;

--Remove redundant column because of not updating with inster_time
ALTER TABLE shujuanyimeng_heat_iqiyi DROP COLUMN timestamp_shanghai;


--Create statistic table of weibo parameters
CREATE TABLE weibo_user_stats (
    id SERIAL PRIMARY KEY,           -- Auto-increment unique ID
    user_id BIGINT NOT NULL,
    followers_count BIGINT,
    followings_count BIGINT,
    likes_count BIGINT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now(),  
    UNIQUE(user_id, recorded_at)      
);


CREATE TABLE weibo_user (        
    user_id BIGINT PRIMARY KEY,
    user_name VARCHAR(20)     
);

INSERT INTO weibo_user (user_id, user_name)
VALUES 
(1265743747, '李兰迪'),
(1798539915, '檀健次'),
(5643994130, '敖瑞鹏');

SHOW TIMEZONE;


