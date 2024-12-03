-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create water_mains table
CREATE TABLE IF NOT EXISTS water_mains (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    dataset_type VARCHAR(100) NOT NULL,
    object_id INTEGER UNIQUE NOT NULL,
    status VARCHAR(50),
    pipe_size NUMERIC,
    material VARCHAR(100),
    installation_date TIMESTAMP,
    pressure_zone VARCHAR(50),
    geometry JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_water_mains_city ON water_mains(city);
CREATE INDEX idx_water_mains_object_id ON water_mains(object_id);
CREATE INDEX idx_water_mains_material ON water_mains(material);
CREATE INDEX idx_water_mains_status ON water_mains(status);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_water_mains_updated_at
    BEFORE UPDATE ON water_mains
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column(); 