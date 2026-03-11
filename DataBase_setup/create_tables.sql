CREATE TABLE finance_voyage_kpi (
    -- Identifiers
    voyage_id VARCHAR(50) NOT NULL,
    voyage_number INTEGER NOT NULL,
    vessel_imo VARCHAR(20),
    
    -- Scenario (ACTUAL or WHEN_FIXED)
    scenario VARCHAR(20) NOT NULL CHECK (scenario IN ('ACTUAL', 'WHEN_FIXED')),
    
    -- Financial KPIs
    revenue NUMERIC(15, 2),
    total_expense NUMERIC(15, 2),
    pnl NUMERIC(15, 2),
    tce NUMERIC(12, 2),
    total_commission NUMERIC(12, 2),
    bunker_cost NUMERIC(12, 2),
    port_cost NUMERIC(12, 2),
    
    -- Time
    voyage_days NUMERIC(10, 4),
    voyage_start_date DATE,
    voyage_end_date DATE,
    
    -- Metadata
    modified_by VARCHAR(100),
    modified_date TIMESTAMP,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Primary Key (voyage_id + scenario)
    PRIMARY KEY (voyage_id, scenario)
);

-- Indexes for performance
CREATE INDEX idx_fvk_voyage_number ON finance_voyage_kpi(voyage_number);
CREATE INDEX idx_fvk_vessel_imo ON finance_voyage_kpi(vessel_imo);
CREATE INDEX idx_fvk_scenario ON finance_voyage_kpi(scenario);
CREATE INDEX idx_fvk_end_date ON finance_voyage_kpi(voyage_end_date);
CREATE INDEX idx_fvk_pnl ON finance_voyage_kpi(pnl DESC NULLS LAST);
CREATE INDEX idx_fvk_tce ON finance_voyage_kpi(tce DESC NULLS LAST);

-- ============================================
-- TABLE 2: OPS_VOYAGE_SUMMARY
-- Purpose: Operational summary for voyages
-- Source: ops_voyages.csv + ops_fixtures.csv + ops_fixture_ports.csv + ops_fixture_grades.csv
-- ============================================

CREATE TABLE ops_voyage_summary (
    -- Identifiers
    voyage_id VARCHAR(50) PRIMARY KEY,
    voyage_number INTEGER NOT NULL,
    vessel_id VARCHAR(50),
    vessel_imo VARCHAR(20),
    vessel_name VARCHAR(100),
    
    -- Voyage Info
    module_type VARCHAR(50),
    fixture_count INTEGER DEFAULT 0,
    offhire_days NUMERIC(10, 4),
    
    -- Delay Info
    is_delayed BOOLEAN DEFAULT FALSE,
    delay_reason TEXT,
    
    -- Time
    voyage_start_date DATE,
    voyage_end_date DATE,
    
    -- JSON Data (aggregated from related tables)
    ports_json JSONB,
    grades_json JSONB,
    activities_json JSONB,
    remarks_json JSONB,
    
    -- Metadata
    tags TEXT,
    url TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_ovs_voyage_number ON ops_voyage_summary(voyage_number);
CREATE INDEX idx_ovs_vessel_imo ON ops_voyage_summary(vessel_imo);
CREATE INDEX idx_ovs_end_date ON ops_voyage_summary(voyage_end_date);
CREATE INDEX idx_ovs_is_delayed ON ops_voyage_summary(is_delayed);
CREATE INDEX idx_ovs_ports_json ON ops_voyage_summary USING GIN(ports_json);
CREATE INDEX idx_ovs_grades_json ON ops_voyage_summary USING GIN(grades_json);
