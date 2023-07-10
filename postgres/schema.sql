CREATE TABLE csv_data (
	id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email_address VARCHAR(320), -- max email length is 64 + @ + 255 = 320 
    created_at TIMESTAMP,
    deleted_at TIMESTAMP,
    merged_at TIMESTAMP,
    parent_user_id BIGINT
);

-- The following are examples to create indexes
-- Add indexes on needed columns based on the business
CREATE INDEX idx_id ON csv_data (id);
CREATE INDEX idx_first_name ON csv_data (first_name);
CREATE INDEX idx_last_name ON csv_data (last_name);
CREATE INDEX idx_email_address ON csv_data (email_address);
CREATE INDEX idx_created_at ON csv_data (created_at);
CREATE INDEX idx_deleted_at ON csv_data (deleted_at);
CREATE INDEX idx_merged_at ON csv_data (merged_at);
CREATE INDEX idx_parent_user_id ON csv_data (parent_user_id);

-- Add a composite index on first_name and last_name if we need it
CREATE INDEX idx_name ON csv_data (first_name, last_name);

-- Add a composite index on email_address and created_at if needed
CREATE INDEX idx_email_created ON csv_data (email_address, created_at);