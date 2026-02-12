---------------------------------------------------------------------------
--2026.2.8 focus on black post
-- Create table of raw date of post(2.0)
CREATE TABLE IF NOT EXISTS public.douban_topic_post_raw
(
    -- Topic / Thread
    topic_id        bigint NOT NULL,             -- topic ID
    topic_title     text,                         -- topic title（with value in OP）
    topic_url       text,                         -- for backtrack / debug

    -- Post identity
    post_type       text NOT NULL,                -- 'op' | 'reply'
    floor_no        integer,                      -- op=0, reply=1,2,3...
    
    -- Author
    user_id         varchar(20),                  -- UID
    user_name       text,
    is_op_author    boolean NOT NULL DEFAULT false,

    -- Content
    content_text    text NOT NULL,                -- raw text without cleaning
    like_count      integer,

    -- Meta
    pubtime         timestamp without time zone,
    ip_location     varchar(20),

    -- Crawl metadata
    crawled_at      timestamp without time zone NOT NULL DEFAULT now(),
    crawler_version text,

    -- Uniqueness
    CONSTRAINT pk_douban_topic_post_raw
        PRIMARY KEY (topic_id, post_type, floor_no)
);

CREATE INDEX IF NOT EXISTS idx_douban_raw_topic_time
ON public.douban_topic_post_raw(topic_id, pubtime);

CREATE INDEX IF NOT EXISTS idx_douban_raw_post_type
ON public.douban_topic_post_raw(post_type);

CREATE INDEX IF NOT EXISTS idx_douban_raw_user
ON public.douban_topic_post_raw(user_id);


--------------------------------------------------------------------
-- 2.9AI analysis table, can be analized many times
--DROP TABLE douban_topic_post_ai;
CREATE TABLE public.douban_topic_post_ai (
  -- Natural key of the original post (one comment)
  user_id         varchar(20),
  topic_id        bigint  NOT NULL,
  post_type       text    NOT NULL,
  floor_no        integer NOT NULL,

  -- Core AI classification results (frequently queried fields)
  ai_label        text    NOT NULL,                 
  -- Classification label: 'hater', 'fan', or 'neutral'

  ai_confidence   numeric(4,3),
  -- Model confidence score in range [0, 1]

  ai_sentiment    text,                             
  -- Overall sentiment: 'positive', 'neutral', 'negative'

  ai_is_sarcasm   boolean,
  -- Whether the comment contains sarcasm or ironic tone

  ai_reason       text,                             
  -- Short natural-language explanation of the classification

  -- Full raw AI output for extensibility and auditing
  ai_result       jsonb   NOT NULL,

  -- Model and prompt versioning
  ai_model        text    NOT NULL,                 
  -- LLM identifier, e.g. 'qwen3:4b'

  prompt_version  text    NOT NULL,                 
  -- Prompt version used for this analysis, e.g. 'v1_qwen3_4b_json'

  -- Timestamp when the comment was labeled by the model
  labeled_at      timestamptz NOT NULL DEFAULT now(),

  -- Composite primary key:
  -- allows the same comment to be analyzed multiple times
  -- using different models or prompt versions
  PRIMARY KEY (topic_id, post_type, floor_no, prompt_version),

  -- Foreign key to the raw post table
  CONSTRAINT fk_ai_post_raw
    FOREIGN KEY (topic_id, post_type, floor_no)
    REFERENCES public.douban_topic_post_raw(topic_id, post_type, floor_no)
    ON DELETE CASCADE,

  -- Data quality constraints
  CONSTRAINT chk_ai_label
    CHECK (ai_label IN ('hater','fan','neutral')),

  CONSTRAINT chk_ai_sentiment
    CHECK (
      ai_sentiment IS NULL
      OR ai_sentiment IN ('positive','neutral','negative')
    )
);
CREATE INDEX IF NOT EXISTS ix_post_ai_user
ON public.douban_topic_post_ai (user_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ai_key
ON public.douban_topic_post_ai(topic_id, post_type, floor_no, prompt_version);

--Add some columns to enrich the label
ALTER TABLE public.douban_topic_post_ai
ADD COLUMN IF NOT EXISTS final_attitude_to_landy text,
ADD COLUMN IF NOT EXISTS ai_is_rebuttal boolean,
ADD COLUMN IF NOT EXISTS ai_mention_negative_claim boolean;

--Backfill historical data (map old ai_labels to new semantics).
UPDATE public.douban_topic_post_ai
SET final_attitude_to_landy =
  CASE ai_label
    WHEN 'fan' THEN 'support'
    WHEN 'hater' THEN 'attack'
    ELSE 'neutral'
  END
WHERE final_attitude_to_landi IS NULL;

ALTER TABLE public.douban_topic_post_ai
ALTER COLUMN final_attitude_to_landy SET NOT NULL;

--Add constraints (make final_attitude_to_landi a required field + restrict the enumeration)
ALTER TABLE public.douban_topic_post_ai
ADD CONSTRAINT chk_final_attitude_to_landy
CHECK (final_attitude_to_landy IN ('support','attack','neutral'));

--Index (makes Superset charts much faster)
CREATE INDEX IF NOT EXISTS ix_post_ai_landy_attitude_time
ON public.douban_topic_post_ai (topic_id, labeled_at, final_attitude_to_landy);

CREATE INDEX IF NOT EXISTS ix_post_ai_rebuttal
ON public.douban_topic_post_ai (ai_is_rebuttal);

-- Optional: If you frequently use the search term "find haters and exclude rebuttals", add a partial index.
CREATE INDEX IF NOT EXISTS ix_post_ai_attack_not_rebuttal
ON public.douban_topic_post_ai (topic_id, labeled_at)
WHERE final_attitude_to_landy = 'attack' AND COALESCE(ai_is_rebuttal,false) = false;

