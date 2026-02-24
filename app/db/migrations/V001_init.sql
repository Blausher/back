CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    is_verified_seller BOOLEAN NOT NULL,
    CHECK (id >= 0)
);

CREATE TABLE advertisements (
    item_id INTEGER PRIMARY KEY,
    seller_id INTEGER NOT NULL REFERENCES users(id),
    name TEXT NOT NULL CHECK (char_length(name) > 0),
    description TEXT NOT NULL CHECK (char_length(description) > 0),
    category INTEGER NOT NULL,
    images_qty INTEGER NOT NULL CHECK (images_qty >= 0),
    CHECK (item_id >= 0),
    CHECK (seller_id >= 0)
);

CREATE TABLE moderation_results (
    id SERIAL PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES advertisements(item_id),
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    is_violation BOOLEAN,
    probability FLOAT,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    CHECK (item_id >= 0)
);


CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    item_id INTEGER NOT NULL CHECK (item_id >= 0),
    moderation_result_id INTEGER NOT NULL UNIQUE REFERENCES moderation_results(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_moderation_results_pending_item_id
    ON moderation_results(item_id)
    WHERE status = 'pending';


ALTER TABLE advertisements
ADD COLUMN is_closed BOOLEAN NOT NULL DEFAULT FALSE;
