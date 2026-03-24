"""
=============================================================================
Synthetic Shipment Dataset Generator
Project: Optimizing Lithium Supply Lanes
         Truck Freight Cost, Service, and Carrier Performance
=============================================================================

This script generates a 24-month historical shipment-level dataset using
three reference files (lane_master, carrier_master, shipment_logic) as inputs.

The output is a clean CSV ready for PostgreSQL import and Tableau analysis.

Author: [Your Name]
Date: March 2026
=============================================================================
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================

# File paths — reference files live in 02_reference, output goes to 04_outputs
REFERENCE_DIR = Path("02_reference:")
OUTPUT_DIR = Path("04_outputs:")

LANE_MASTER_FILE = REFERENCE_DIR / "lane_master.xlsx"
CARRIER_MASTER_FILE = REFERENCE_DIR / "carrier_master.xlsx"
SHIPMENT_LOGIC_FILE = REFERENCE_DIR / "shipment_logic.xlsx"
OUTPUT_FILE = OUTPUT_DIR / "synthetic_shipments_24m.csv"

# Time range: 24 months ending March 2026
START_YEAR = 2024
START_MONTH = 4
NUM_MONTHS = 24

# Random seed for reproducibility
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)


# =============================================================================
# SECTION 1: LOAD REFERENCE FILES
# =============================================================================

print("=" * 60)
print("SECTION 1: Loading reference files...")
print("=" * 60)

lane_master = pd.read_excel(LANE_MASTER_FILE)
carrier_master = pd.read_excel(CARRIER_MASTER_FILE)
shipment_logic = pd.read_excel(SHIPMENT_LOGIC_FILE)

# Standardize column names — strip whitespace and lowercase
# Prevents silent mismatches from trailing spaces or capitalization
lane_master.columns = lane_master.columns.str.strip().str.lower()
carrier_master.columns = carrier_master.columns.str.strip().str.lower()
shipment_logic.columns = shipment_logic.columns.str.strip().str.lower()

print(f"  Lane master:    {len(lane_master)} lanes loaded")
print(f"  Carrier master: {len(carrier_master)} carriers loaded")
print(f"  Shipment logic: {len(shipment_logic)} lane configs loaded")
print()


# =============================================================================
# SECTION 2: VALIDATE REFERENCE FILES
# =============================================================================

print("=" * 60)
print("SECTION 2: Validating reference files...")
print("=" * 60)

# Check that lane IDs match across lane_master and shipment_logic
lane_ids_master = set(lane_master['lane_id'])
lane_ids_logic = set(shipment_logic['lane_id'])

if lane_ids_master != lane_ids_logic:
    missing_in_logic = lane_ids_master - lane_ids_logic
    missing_in_master = lane_ids_logic - lane_ids_master
    if missing_in_logic:
        print(f"  WARNING: Lanes in master but missing in logic: {missing_in_logic}")
    if missing_in_master:
        print(f"  WARNING: Lanes in logic but missing in master: {missing_in_master}")
    raise ValueError("Lane IDs do not match across reference files. Fix before proceeding.")
else:
    print("  PASS: Lane IDs match across lane_master and shipment_logic")

# Check for missing values in critical columns
critical_lane_cols = ['lane_id', 'origin_state', 'destination_state', 'assumed_distance_miles']
critical_carrier_cols = ['carrier_id', 'carrier_name', 'base_cost_index', 'base_otif_index', 'base_exception_index']
critical_logic_cols = ['lane_id', 'monthly_shipment_base', 'avg_weight_lbs', 'base_cost_per_mile',
                       'planned_transit_days', 'delay_risk_pct', 'invoice_exception_pct']

for col in critical_lane_cols:
    if lane_master[col].isnull().any():
        raise ValueError(f"Missing values found in lane_master column: {col}")

for col in critical_carrier_cols:
    if carrier_master[col].isnull().any():
        raise ValueError(f"Missing values found in carrier_master column: {col}")

for col in critical_logic_cols:
    if shipment_logic[col].isnull().any():
        raise ValueError(f"Missing values found in shipment_logic column: {col}")

print("  PASS: No missing values in critical columns")

# Validate carrier allocation caps are reasonable
# NOTE: allocation_cap_pct values are used as relative weights for probabilistic
# assignment, not as strict caps. This is weighted assignment, not cap enforcement.
total_cap = carrier_master['allocation_cap_pct'].sum()
print(f"  INFO: Total carrier allocation cap sum = {total_cap:.2f} (used as relative weights, not strict caps)")
print("  PASS: All validations passed")
print()


# =============================================================================
# SECTION 3: MERGE REFERENCE DATA
# =============================================================================

print("=" * 60)
print("SECTION 3: Merging reference data...")
print("=" * 60)

# Merge lane_master with shipment_logic on lane_id
lane_config = lane_master.merge(shipment_logic, on='lane_id', how='inner')

print(f"  Merged lane config: {len(lane_config)} rows")
print()


# =============================================================================
# SECTION 4: BUILD CARRIER ASSIGNMENT WEIGHTS
# =============================================================================

print("=" * 60)
print("SECTION 4: Building carrier assignment weights...")
print("=" * 60)

# Use allocation_cap_pct as relative weights for probabilistic assignment
# This means Titan Logistics (0.40) gets proportionally more volume,
# Velocity Haul (0.25) gets less — reflecting real-world carrier trust levels
carrier_weights = carrier_master['allocation_cap_pct'].values
carrier_weights_normalized = carrier_weights / carrier_weights.sum()

carrier_ids = carrier_master['carrier_id'].values
carrier_names = carrier_master['carrier_name'].values
carrier_profiles = carrier_master['carrier_profile'].values

print("  Carrier assignment probabilities (weighted, not capped):")
for i, cid in enumerate(carrier_ids):
    print(f"    {cid} ({carrier_names[i]}): {carrier_weights_normalized[i]:.2%}")
print()


# =============================================================================
# SECTION 5: GENERATE SHIPMENT RECORDS
# =============================================================================

print("=" * 60)
print("SECTION 5: Generating shipment records...")
print("=" * 60)

# Build a lookup dictionary for carrier-level attributes
carrier_lookup = carrier_master.set_index('carrier_id').to_dict('index')

all_shipments = []
shipment_counter = 0

# Loop through each month in the 24-month window
for month_offset in range(NUM_MONTHS):
    year = START_YEAR + (START_MONTH + month_offset - 1) // 12
    month = (START_MONTH + month_offset - 1) % 12 + 1
    quarter = (month - 1) // 3 + 1

    # Determine number of days in this month for date distribution
    if month == 12:
        days_in_month = (pd.Timestamp(year + 1, 1, 1) - pd.Timestamp(year, month, 1)).days
    else:
        days_in_month = (pd.Timestamp(year, month + 1, 1) - pd.Timestamp(year, month, 1)).days

    # Loop through each lane
    for _, lane in lane_config.iterrows():
        lane_id = lane['lane_id']
        origin_state = lane['origin_state']
        destination_state = lane['destination_state']
        origin_type = lane['origin_type']
        destination_type = lane['destination_type']
        distance_miles = lane['assumed_distance_miles']
        volume_band = lane['volume_band']
        service_sensitivity = lane['service_sensitivity']
        base_risk_band = lane['base_risk_band']
        strategic_priority = lane['strategic_priority']
        priority_flag = lane['priority_flag']

        # Shipment logic parameters
        monthly_base = lane['monthly_shipment_base']
        avg_weight = lane['avg_weight_lbs']
        weight_var_pct = lane['weight_variation_pct']
        base_cpm = lane['base_cost_per_mile']
        planned_transit = lane['planned_transit_days']
        delay_risk = lane['delay_risk_pct']
        exception_pct = lane['invoice_exception_pct']
        avg_util = lane['avg_utilization_pct']

        # Add slight monthly volume variation (+/- 15%)
        num_shipments = max(1, int(np.random.normal(monthly_base, monthly_base * 0.15)))

        # Generate shipments for this lane-month
        for _ in range(num_shipments):
            shipment_counter += 1
            shipment_id = f"SH{shipment_counter:06d}"

            # --- Shipment Date ---
            # Distribute randomly across the month
            day = np.random.randint(1, days_in_month + 1)
            shipment_date = pd.Timestamp(year, month, day)
            month_name = shipment_date.strftime('%B')

            # --- Carrier Assignment ---
            # Probabilistic based on allocation weights
            carrier_idx = np.random.choice(len(carrier_ids), p=carrier_weights_normalized)
            carrier_id = carrier_ids[carrier_idx]
            carrier_name = carrier_names[carrier_idx]
            carrier_profile = carrier_profiles[carrier_idx]
            carrier_data = carrier_lookup[carrier_id]

            # --- Weight ---
            # Normal distribution around lane average with lane-specific variation
            weight_lbs = max(2000, np.random.normal(avg_weight, avg_weight * weight_var_pct))
            weight_lbs = round(weight_lbs, 0)

            # --- Quoted Cost ---
            # Base formula: distance * base_cost_per_mile * carrier_cost_index
            # Add slight random noise (+/- 5%)
            carrier_cost_idx = carrier_data['base_cost_index']
            quoted_cost = distance_miles * base_cpm * carrier_cost_idx
            quoted_cost *= np.random.uniform(0.95, 1.05)
            quoted_cost = round(max(200, quoted_cost), 2)

            # --- Actual Cost ---
            # Quoted cost + variation influenced by carrier exception tendency
            # Higher exception carriers tend to have more cost overruns
            carrier_exception_idx = carrier_data['base_exception_index']
            cost_variation_factor = np.random.normal(1.0, 0.03 + carrier_exception_idx * 0.5)
            # Occasionally add larger overruns for exception-prone scenarios
            if np.random.random() < carrier_exception_idx:
                cost_variation_factor += np.random.uniform(0.02, 0.12)
            actual_cost = quoted_cost * cost_variation_factor
            actual_cost = round(max(200, actual_cost), 2)

            # --- Cost Leakage ---
            cost_leakage_amount = round(actual_cost - quoted_cost, 2)

            # --- Planned Transit Days ---
            planned_transit_days = planned_transit

            # --- Actual Transit Days ---
            # Combines lane delay_risk with carrier OTIF index
            # Higher OTIF carriers reduce the effective delay probability
            carrier_otif_idx = carrier_data['base_otif_index']
            otif_adjustment = (carrier_otif_idx - 0.85) * 2  # Normalizes around baseline
            effective_delay_risk = max(0.01, delay_risk * (1 - otif_adjustment))

            if np.random.random() < effective_delay_risk:
                # Delay occurs: add 1-3 extra days
                delay_days = np.random.choice([1, 2, 3], p=[0.55, 0.30, 0.15])
                actual_transit_days = planned_transit + delay_days
            else:
                # No delay: might even arrive slightly early
                if np.random.random() < 0.15:
                    actual_transit_days = max(1, planned_transit - 1)
                else:
                    actual_transit_days = planned_transit

            # --- On-Time Flag ---
            on_time_flag = 1 if actual_transit_days <= planned_transit_days else 0

            # --- Invoice Exception Flag ---
            # Combine lane exception rate and carrier exception index
            # NOTE: using 0.3 multiplier — if output exception rates look too high,
            # reduce to 0.2: combined_exception_rate = exception_pct + carrier_exception_idx * 0.2
            combined_exception_rate = exception_pct + carrier_exception_idx * 0.3
            combined_exception_rate = min(combined_exception_rate, 0.35)  # Cap at 35%
            invoice_exception_flag = 1 if np.random.random() < combined_exception_rate else 0

            # --- Utilization ---
            # Normal distribution around lane average, clamped 40%-100%
            utilization_pct = np.random.normal(avg_util, 0.06)
            utilization_pct = round(np.clip(utilization_pct, 0.40, 1.00), 2)

            # --- Build Record ---
            record = {
                'shipment_id': shipment_id,
                'shipment_date': shipment_date,
                'year': year,
                'month': month,
                'month_name': month_name,
                'quarter': quarter,
                'lane_id': lane_id,
                'origin_state': origin_state,
                'destination_state': destination_state,
                'origin_type': origin_type,
                'destination_type': destination_type,
                'volume_band': volume_band,
                'service_sensitivity': service_sensitivity,
                'base_risk_band': base_risk_band,
                'strategic_priority': strategic_priority,
                'carrier_id': carrier_id,
                'carrier_name': carrier_name,
                'carrier_profile': carrier_profile,
                'distance_miles': distance_miles,
                'weight_lbs': weight_lbs,
                'planned_transit_days': planned_transit_days,
                'actual_transit_days': actual_transit_days,
                'quoted_cost': quoted_cost,
                'actual_cost': actual_cost,
                'cost_leakage_amount': cost_leakage_amount,
                'on_time_flag': on_time_flag,
                'invoice_exception_flag': invoice_exception_flag,
                'utilization_pct': utilization_pct,
                'priority_flag': priority_flag
            }
            all_shipments.append(record)

print(f"  Total shipments generated: {len(all_shipments)}")
print()


# =============================================================================
# SECTION 6: POST-PROCESSING AND QUALITY CHECKS
# =============================================================================

print("=" * 60)
print("SECTION 6: Post-processing and quality checks...")
print("=" * 60)

# Convert to DataFrame
df = pd.DataFrame(all_shipments)

# Sort by shipment date then shipment ID
df = df.sort_values(['shipment_date', 'shipment_id']).reset_index(drop=True)

# --- Quality Checks ---

# Check 1: No negative costs
assert (df['quoted_cost'] > 0).all(), "FAIL: Negative quoted costs found"
assert (df['actual_cost'] > 0).all(), "FAIL: Negative actual costs found"
print("  PASS: No negative costs")

# Check 2: No negative weights
assert (df['weight_lbs'] > 0).all(), "FAIL: Negative weights found"
print("  PASS: No negative weights")

# Check 3: Utilization within bounds
assert (df['utilization_pct'] >= 0.40).all(), "FAIL: Utilization below 40%"
assert (df['utilization_pct'] <= 1.00).all(), "FAIL: Utilization above 100%"
print("  PASS: Utilization within 40%-100% bounds")

# Check 4: Actual transit days are positive and realistic
assert (df['actual_transit_days'] >= 1).all(), "FAIL: Transit days below 1"
assert (df['actual_transit_days'] <= 15).all(), "FAIL: Unrealistic transit days (>15)"
print("  PASS: Transit days within realistic range")

# Check 5: No missing values in critical columns
critical_output_cols = ['shipment_id', 'shipment_date', 'lane_id', 'carrier_id',
                        'quoted_cost', 'actual_cost', 'on_time_flag']
for col in critical_output_cols:
    assert df[col].isnull().sum() == 0, f"FAIL: Missing values in {col}"
print("  PASS: No missing values in critical output columns")

# Check 6: No duplicate shipment IDs
assert df['shipment_id'].nunique() == len(df), "FAIL: Duplicate shipment IDs found"
print("  PASS: No duplicate shipment IDs")

# Check 7: Date range coverage
date_range = f"{df['shipment_date'].min().strftime('%Y-%m-%d')} to {df['shipment_date'].max().strftime('%Y-%m-%d')}"
print(f"  INFO: Date range covered: {date_range}")

# Check 8: Lane and carrier distribution
print(f"  INFO: Unique lanes: {df['lane_id'].nunique()}")
print(f"  INFO: Unique carriers: {df['carrier_id'].nunique()}")
print()


# =============================================================================
# SECTION 7: SUMMARY STATISTICS
# =============================================================================

print("=" * 60)
print("SECTION 7: Summary statistics...")
print("=" * 60)

print(f"\n  Total shipments:          {len(df):,}")
print(f"  Total freight spend:      ${df['actual_cost'].sum():,.2f}")
print(f"  Avg quoted cost:          ${df['quoted_cost'].mean():,.2f}")
print(f"  Avg actual cost:          ${df['actual_cost'].mean():,.2f}")
print(f"  Avg cost leakage:         ${df['cost_leakage_amount'].mean():,.2f}")
print(f"  Overall OTIF rate:        {df['on_time_flag'].mean():.2%}")
print(f"  Invoice exception rate:   {df['invoice_exception_flag'].mean():.2%}")
print(f"  Avg utilization:          {df['utilization_pct'].mean():.2%}")
print(f"  Avg weight (lbs):         {df['weight_lbs'].mean():,.0f}")

print("\n  Shipments by lane:")
lane_counts = df.groupby('lane_id').size().sort_values(ascending=False)
for lane_id, count in lane_counts.items():
    print(f"    {lane_id}: {count:,} shipments")

print("\n  Shipments by carrier:")
carrier_counts = df.groupby(['carrier_id', 'carrier_name']).size().sort_values(ascending=False)
for (cid, cname), count in carrier_counts.items():
    print(f"    {cid} ({cname}): {count:,} shipments ({count/len(df):.1%})")

print("\n  OTIF by carrier:")
otif_by_carrier = df.groupby(['carrier_id', 'carrier_name'])['on_time_flag'].mean()
for (cid, cname), otif in otif_by_carrier.items():
    print(f"    {cid} ({cname}): {otif:.2%}")

print("\n  Exception rate by carrier:")
exc_by_carrier = df.groupby(['carrier_id', 'carrier_name'])['invoice_exception_flag'].mean()
for (cid, cname), exc in exc_by_carrier.items():
    print(f"    {cid} ({cname}): {exc:.2%}")
print()


# =============================================================================
# SECTION 8: EXPORT TO CSV
# =============================================================================

print("=" * 60)
print("SECTION 8: Exporting to CSV...")
print("=" * 60)

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Export
df.to_csv(OUTPUT_FILE, index=False)

print(f"  Exported: {OUTPUT_FILE}")
print(f"  Rows:     {len(df):,}")
print(f"  Columns:  {len(df.columns)}")
print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")
print()
print("=" * 60)
print("DONE — Dataset generation complete.")
print("=" * 60)