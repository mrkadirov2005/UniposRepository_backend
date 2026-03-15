-- Create wagons table
CREATE TABLE IF NOT EXISTS wagons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wagon_number VARCHAR(100) NOT NULL,
    products JSONB NOT NULL DEFAULT '[]'::jsonb,
    total DECIMAL(15, 2) DEFAULT 0,
    indicator VARCHAR(50) NOT NULL CHECK (indicator IN ('debt_taken', 'debt_given', 'none')),
    shop_id UUID,
    branch VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by UUID,
    CONSTRAINT unique_wagon_number UNIQUE (wagon_number)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_wagons_wagon_number ON wagons(wagon_number);
CREATE INDEX IF NOT EXISTS idx_wagons_shop_id ON wagons(shop_id);
CREATE INDEX IF NOT EXISTS idx_wagons_indicator ON wagons(indicator);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_wagon_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_wagon_timestamp
    BEFORE UPDATE ON wagons
    FOR EACH ROW
    EXECUTE FUNCTION update_wagon_timestamp();

-- Comments for documentation
COMMENT ON TABLE wagons IS 'Stores wagon information with products and debt indicators';
COMMENT ON COLUMN wagons.wagon_number IS 'Unique wagon identifier/number';
COMMENT ON COLUMN wagons.products IS 'JSONB array of products with structure: [{product_id, product_name, amount, price, subtotal}]';
COMMENT ON COLUMN wagons.total IS 'Total value of all products in the wagon';
COMMENT ON COLUMN wagons.indicator IS 'Debt type indicator: debt_taken (I took debt), debt_given (I gave debt), or none';
COMMENT ON COLUMN wagons.shop_id IS 'Reference to shop if applicable';
COMMENT ON COLUMN wagons.branch IS 'Branch identifier';
