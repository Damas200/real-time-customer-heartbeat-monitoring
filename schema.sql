CREATE TABLE customer_heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    heart_rate NUMERIC(5,2) NOT NULL,
    is_anomaly BOOLEAN DEFAULT FALSE,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_event UNIQUE (customer_id, event_timestamp)
);

CREATE INDEX idx_customer_id ON customer_heartbeats(customer_id);
CREATE INDEX idx_event_timestamp ON customer_heartbeats(event_timestamp);

