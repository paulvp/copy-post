CREATE TABLE IF NOT EXISTS content (
    id BIGSERIAL PRIMARY KEY,
    text TEXT NOT NULL,
    category TEXT,
    media_links TEXT[] DEFAULT '{}',
    media_type TEXT,
    message_id BIGINT,
    channel_id TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(message_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_content_category ON content(category);
CREATE INDEX IF NOT EXISTS idx_content_created_at ON content(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_content_media_type ON content(media_type);
CREATE INDEX IF NOT EXISTS idx_content_message_channel ON content(message_id, channel_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_content_updated_at ON content;
CREATE TRIGGER update_content_updated_at 
    BEFORE UPDATE ON content 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE VIEW recent_content AS
SELECT 
    id,
    text,
    category,
    media_links,
    media_type,
    message_id,
    channel_id,
    created_at,
    updated_at
FROM content
ORDER BY created_at DESC;
