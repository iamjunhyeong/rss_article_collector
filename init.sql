-- init.sql
CREATE TABLE IF NOT EXISTS article_tag (
  article_id BIGINT PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
  categories TEXT[] NOT NULL,
  sentiment TEXT NOT NULL CHECK (
    sentiment IN (
      'hope_encourage',
      'anger_criticism',
      'anxiety_crisis',
      'sad_shock',
      'neutral_factual'
    )
  ),
  confidence REAL,
  rationale TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
