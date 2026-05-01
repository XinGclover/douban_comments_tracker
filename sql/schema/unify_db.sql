ALTER TABLE zhaoxuelu_comments
ADD COLUMN IF NOT EXISTS source_drama_id VARCHAR(20);

ALTER TABLE zhaoxuelu_comments_count
ADD COLUMN IF NOT EXISTS source_drama_id VARCHAR(20);

ALTER TABLE zhaoxuelu_heat_iqiyi
ADD COLUMN IF NOT EXISTS source_drama_id VARCHAR(20);

ALTER TABLE zhaoxuelu_heat_iqiyi
ADD COLUMN IF NOT EXISTS iqiyi_id VARCHAR(50);



UPDATE zhaoxuelu_comments
SET source_drama_id = '36317401'
WHERE source_drama_id IS NULL;

UPDATE zhaoxuelu_comments_count
SET source_drama_id = '36317401'
WHERE source_drama_id IS NULL;

UPDATE zhaoxuelu_heat_iqiyi
SET 
    source_drama_id = '36317401',
    iqiyi_id = 'a_j7cnht5c2t'
WHERE source_drama_id IS NULL;


ALTER TABLE zhaoxuelu_comments
ALTER COLUMN source_drama_id SET NOT NULL;

ALTER TABLE zhaoxuelu_comments_count
ALTER COLUMN source_drama_id SET NOT NULL;

ALTER TABLE zhaoxuelu_heat_iqiyi
ALTER COLUMN source_drama_id SET NOT NULL;

ALTER TABLE zhaoxuelu_heat_iqiyi
ALTER COLUMN iqiyi_id SET NOT NULL;