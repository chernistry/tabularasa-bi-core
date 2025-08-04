-- V3__Create_campaign_metadata_table.sql
-- Create campaign metadata table for enhanced analytics

CREATE TABLE IF NOT EXISTS campaign_metadata (
    id BIGSERIAL PRIMARY KEY,
    campaign_id VARCHAR(255) UNIQUE NOT NULL,
    campaign_name VARCHAR(500) NOT NULL,
    advertiser_id VARCHAR(255) NOT NULL,
    advertiser_name VARCHAR(500) NOT NULL,
    industry_category VARCHAR(100),
    target_audience VARCHAR(255),
    start_date DATE NOT NULL,
    end_date DATE,
    budget_usd DECIMAL(15, 2),
    target_ctr DECIMAL(5, 4),
    target_cpa DECIMAL(10, 2),
    is_active BOOLEAN NOT NULL DEFAULT true,
    campaign_type VARCHAR(50) NOT NULL DEFAULT 'standard',
    priority INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT campaign_metadata_dates_check CHECK (end_date IS NULL OR end_date >= start_date),
    CONSTRAINT campaign_metadata_budget_check CHECK (budget_usd IS NULL OR budget_usd >= 0),
    CONSTRAINT campaign_metadata_priority_check CHECK (priority BETWEEN 1 AND 10),
    CONSTRAINT campaign_metadata_ctr_check CHECK (target_ctr IS NULL OR (target_ctr >= 0 AND target_ctr <= 1)),
    CONSTRAINT campaign_metadata_type_check CHECK (campaign_type IN ('standard', 'premium', 'test', 'pilot'))
);

-- Create indexes for optimal query performance
CREATE INDEX idx_campaign_metadata_campaign_id 
    ON campaign_metadata (campaign_id);

CREATE INDEX idx_campaign_metadata_advertiser_id 
    ON campaign_metadata (advertiser_id);

CREATE INDEX idx_campaign_metadata_is_active 
    ON campaign_metadata (is_active);

CREATE INDEX idx_campaign_metadata_dates 
    ON campaign_metadata (start_date, end_date);

CREATE INDEX idx_campaign_metadata_industry 
    ON campaign_metadata (industry_category);

CREATE INDEX idx_campaign_metadata_type_priority 
    ON campaign_metadata (campaign_type, priority);

-- Create composite index for common query patterns
CREATE INDEX idx_campaign_metadata_active_advertiser 
    ON campaign_metadata (is_active, advertiser_id, campaign_type);

-- Add table and column comments
COMMENT ON TABLE campaign_metadata IS 'Master data for advertising campaigns including targeting and budget information';
COMMENT ON COLUMN campaign_metadata.campaign_id IS 'Unique identifier matching the campaign_id in aggregated_campaign_stats';
COMMENT ON COLUMN campaign_metadata.advertiser_id IS 'Identifier for the advertiser/client';
COMMENT ON COLUMN campaign_metadata.industry_category IS 'Industry vertical (e.g., Technology, Retail, Finance)';
COMMENT ON COLUMN campaign_metadata.target_audience IS 'Description of target demographic or audience';
COMMENT ON COLUMN campaign_metadata.target_ctr IS 'Target click-through rate (0.0 to 1.0)';
COMMENT ON COLUMN campaign_metadata.target_cpa IS 'Target cost per acquisition in USD';
COMMENT ON COLUMN campaign_metadata.campaign_type IS 'Type of campaign: standard, premium, test, or pilot';
COMMENT ON COLUMN campaign_metadata.priority IS 'Campaign priority from 1 (highest) to 10 (lowest)';

-- Insert sample data for development and testing
INSERT INTO campaign_metadata (
    campaign_id, campaign_name, advertiser_id, advertiser_name, 
    industry_category, target_audience, start_date, end_date, 
    budget_usd, target_ctr, target_cpa, campaign_type, priority
) VALUES 
    ('campaign_001', 'Summer Tech Sale 2025', 'adv_001', 'TechCorp Inc', 
     'Technology', 'Tech enthusiasts 25-45', '2025-06-01', '2025-08-31', 
     50000.00, 0.0350, 25.00, 'standard', 2),
    ('campaign_002', 'Fashion Forward Spring', 'adv_002', 'StyleBrand Ltd', 
     'Fashion', 'Fashion-conscious women 18-35', '2025-03-01', '2025-05-31', 
     75000.00, 0.0280, 30.00, 'premium', 1),
    ('campaign_003', 'Fitness New Year Push', 'adv_003', 'FitLife Co', 
     'Health & Fitness', 'Health-conscious adults 25-55', '2025-01-01', '2025-02-28', 
     30000.00, 0.0420, 20.00, 'standard', 3),
    ('campaign_004', 'B2B Software Launch', 'adv_001', 'TechCorp Inc', 
     'Technology', 'IT decision makers 30-50', '2025-01-15', NULL, 
     100000.00, 0.0150, 150.00, 'premium', 1),
    ('campaign_005', 'Holiday Pilot Test', 'adv_004', 'RetailGiant Corp', 
     'Retail', 'Holiday shoppers all ages', '2024-11-01', '2024-12-31', 
     10000.00, 0.0300, 15.00, 'pilot', 5),
    ('campaign_006', 'Financial Services Q1', 'adv_005', 'MoneyWise Bank', 
     'Finance', 'High-income professionals 35-65', '2025-01-01', '2025-03-31', 
     80000.00, 0.0200, 75.00, 'standard', 2),
    ('campaign_007', 'Auto Insurance Renewal', 'adv_006', 'SafeDrive Insurance', 
     'Insurance', 'Car owners 25-65', '2025-02-01', '2025-04-30', 
     45000.00, 0.0380, 35.00, 'standard', 3),
    ('campaign_008', 'Gaming Console Promo', 'adv_007', 'GameZone Electronics', 
     'Gaming', 'Gamers 16-40', '2025-03-15', '2025-06-15', 
     60000.00, 0.0450, 22.00, 'standard', 2),
    ('campaign_009', 'Premium Travel Package', 'adv_008', 'LuxTravel Agency', 
     'Travel', 'Affluent travelers 35-65', '2025-04-01', '2025-09-30', 
     120000.00, 0.0180, 200.00, 'premium', 1),
    ('campaign_010', 'Education Platform Test', 'adv_009', 'LearnMore EdTech', 
     'Education', 'Students and professionals 18-45', '2025-01-10', '2025-02-10', 
     15000.00, 0.0320, 40.00, 'test', 4)
ON CONFLICT (campaign_id) DO NOTHING;

-- Create function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_campaign_metadata_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update updated_at on row changes
CREATE TRIGGER trigger_campaign_metadata_updated_at
    BEFORE UPDATE ON campaign_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_campaign_metadata_updated_at();