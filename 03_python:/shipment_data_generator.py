"""
=============================================================================
Synthetic Shipment Dataset Generator — v2 (ML-Optimized)
Project: Optimizing Lithium Supply Lanes
         Truck Freight Cost, Service, and Carrier Performance
=============================================================================

v2 Changes from v1:
  - Delay probability now driven systematically by features:
    * carrier profile (strong effect)
    * lane distance (compounding effect)
    * utilization (low util = higher delay risk)
    * weight extremes (very heavy or very light = higher risk)
    * seasonal patterns (winter months = higher delays)
    * cost-delay correlation (cost overruns correlate with delays)
  - All columns, lane IDs, carrier IDs, and structure are IDENTICAL to v1
  - SQL queries, Layer 2 views, and Layer 3 script need NO changes

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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REFERENCE_DIR = PROJECT_ROOT / "02_reference:"
OUTPUT_DIR = PROJECT_ROOT / "04_outputs:"

LANE_MASTER_FILE = REFERENCE_DIR / "lane_master.xlsx"
CARRIER_MASTER_FILE = REFERENCE_DIR / "carrier_master.xlsx"
SHIPMENT_LOGIC_FILE = REFERENCE_DIR / "shipment_logic.xlsx"
OUTPUT_FILE = OUTPUT_DIR / "synthetic_shipments_24m.csv"

START_YEAR = 2024
START_MONTH = 4
NUM_MONTHS = 24

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

lane_ids_master = set(lane_master['lane_id'])
lane_ids_logic = set(shipment_logic['lane_id'])

if lane_ids_master != lane_ids_logic:
    missing_in_logic = lane_ids_master - lane_ids_logic
    missing_in_master = lane_ids_logic - lane_ids_master
    if missing_in_logic:
        print(f"  WARNING: Lanes in master but missing in logic: {missing_in_logic}")
    if missing_in_master:
        print(f"  WARNING: Lanes in logic but missing in master: {missing_in_master}")
    raise ValueError("Lane IDs do not match across reference files.")
else:
    print("  PASS: Lane IDs match across lane_master and shipment_logic")

critical_lane_cols = ['lane_id', 'origin_state', 'destination_state', 'assumed_distance_miles']
critical_carrier_cols = ['carrier_id', 'carrier_name', 'base_cost_index', 'base_otif_index', 'base_exception_index']
critical_logic_cols = ['lane_id', 'monthly_shipment_base', 'avg_weight_lbs', 'base_cost_per_mile',
                       'planned_transit_days', 'delay_risk_pct', 'invoice_exception_pct']

for col in critical_lane_cols:
    if lane_master[col].isnull().any():
        raise ValueError(f"Missing values in lane_master: {col}")

for col in critical_carrier_cols:
    if carrier_master[col].isnull().any():
        raise ValueError(f"Missing values in carrier_master: {col}")

for col in critical_logic_cols:
    if shipment_logic[col].isnull().any():
        raise ValueError(f"Missing values in shipment_logic: {col}")

print("  PASS: No missing values in critical columns")

total_cap = carrier_master['allocation_cap_pct'].sum()
print(f"  INFO: Total carrier allocation cap sum = {total_cap:.2f} (used as relative weights)")
print("  PASS: All validations passed")
print()


# =============================================================================
# SECTION 3: MERGE REFERENCE DATA
# =============================================================================

print("=" * 60)
print("SECTION 3: Merging reference data...")
print("=" * 60)

lane_config = lane_master.merge(shipment_logic, on='lane_id', how='inner')
print(f"  Merged lane config: {len(lane_config)} rows")
print()


# =============================================================================
# SECTION 4: BUILD CARRIER ASSIGNMENT WEIGHTS
# =============================================================================

print("=" * 60)
print("SECTION 4: Building carrier assignment weights...")
print("=" * 60)

carrier_weights = carrier_master['allocation_cap_pct'].values
carrier_weights_normalized = carrier_weights / carrier_weights.sum()

carrier_ids = carrier_master['carrier_id'].values
carrier_names = carrier_master['carrier_name'].values
carrier_profiles = carrier_master['carrier_profile'].values

print("  Carrier assignment probabilities (weighted):")
for i, cid in enumerate(carrier_ids):
    print(f"    {cid} ({carrier_names[i]}): {carrier_weights_normalized[i]:.2%}")
print()


# =============================================================================
# SECTION 5: GENERATE SHIPMENT RECORDS (v2 — ML-OPTIMIZED)
# =============================================================================

print("=" * 60)
print("SECTION 5: Generating shipment records (v2 — ML-optimized)...")
print("=" * 60)

carrier_lookup = carrier_master.set_index('carrier_id').to_dict('index')

# --- v2: Carrier delay multipliers (strong effect) ---
# These create clear, learnable differences between carriers
carrier_delay_multiplier = {
    'C01': 1.4,    # Apex Freight — cheap but higher delay risk
    'C02': 0.4,    # Titan Logistics — premium, very reliable
    'C03': 0.8,    # BlueRoute — balanced
    'C04': 2.0,    # Velocity Haul — exception-prone, highest delay risk
}

# --- v2: Seasonal delay multipliers ---
# Winter and peak months have more delays
seasonal_multiplier = {
    1: 1.6,   # January — winter
    2: 1.5,   # February — winter
    3: 1.2,   # March — transition
    4: 0.8,   # April — mild
    5: 0.7,   # May — best conditions
    6: 0.8,   # June — mild
    7: 1.0,   # July — summer heat
    8: 1.1,   # August — summer heat
    9: 0.9,   # September — mild
    10: 1.0,  # October — normal
    11: 1.3,  # November — pre-winter + holiday freight surge
    12: 1.7,  # December — winter + holiday peak
}

# --- v2: Distance normalization for compounding risk ---
max_distance = lane_config['assumed_distance_miles'].max()

all_shipments = []
shipment_counter = 0

for month_offset in range(NUM_MONTHS):
    year = START_YEAR + (START_MONTH + month_offset - 1) // 12
    month = (START_MONTH + month_offset - 1) % 12 + 1
    quarter = (month - 1) // 3 + 1

    if month == 12:
        days_in_month = (pd.Timestamp(year + 1, 1, 1) - pd.Timestamp(year, month, 1)).days
    else:
        days_in_month = (pd.Timestamp(year, month + 1, 1) - pd.Timestamp(year, month, 1)).days

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

        monthly_base = lane['monthly_shipment_base']
        avg_weight = lane['avg_weight_lbs']
        weight_var_pct = lane['weight_variation_pct']
        base_cpm = lane['base_cost_per_mile']
        planned_transit = lane['planned_transit_days']
        delay_risk = lane['delay_risk_pct']
        exception_pct = lane['invoice_exception_pct']
        avg_util = lane['avg_utilization_pct']

        # Monthly volume variation (+/- 15%)
        num_shipments = max(1, int(np.random.normal(monthly_base, monthly_base * 0.15)))

        for _ in range(num_shipments):
            shipment_counter += 1
            shipment_id = f"SH{shipment_counter:06d}"

            # --- Shipment Date ---
            day = np.random.randint(1, days_in_month + 1)
            shipment_date = pd.Timestamp(year, month, day)
            month_name = shipment_date.strftime('%B')

            # --- Carrier Assignment ---
            carrier_idx = np.random.choice(len(carrier_ids), p=carrier_weights_normalized)
            carrier_id = carrier_ids[carrier_idx]
            carrier_name = carrier_names[carrier_idx]
            carrier_profile = carrier_profiles[carrier_idx]
            carrier_data = carrier_lookup[carrier_id]

            # --- Weight ---
            weight_lbs = max(2000, np.random.normal(avg_weight, avg_weight * weight_var_pct))
            weight_lbs = round(weight_lbs, 0)

            # --- Utilization ---
            utilization_pct = np.random.normal(avg_util, 0.06)
            utilization_pct = round(np.clip(utilization_pct, 0.40, 1.00), 2)

            # --- Quoted Cost ---
            carrier_cost_idx = carrier_data['base_cost_index']
            quoted_cost = distance_miles * base_cpm * carrier_cost_idx
            quoted_cost *= np.random.uniform(0.95, 1.05)
            quoted_cost = round(max(200, quoted_cost), 2)

            # =============================================================
            # v2: SYSTEMATIC DELAY PROBABILITY
            # =============================================================
            # Instead of a flat random coin flip, delay probability is now
            # driven by compounding factors the model can learn from.
            #
            # Base delay risk comes from the lane's delay_risk_pct,
            # then multiplied by:
            #   1. Carrier reliability (strong effect: 0.4x to 2.0x)
            #   2. Distance factor (longer = riskier, up to 1.5x)
            #   3. Seasonal factor (winter/peak = riskier, up to 1.7x)
            #   4. Utilization factor (very low or very high = riskier)
            #   5. Weight factor (extreme weights = riskier)
            # =============================================================

            # Factor 1: Carrier delay multiplier (strong, learnable)
            carrier_delay_factor = carrier_delay_multiplier[carrier_id]

            # Factor 2: Distance compounding (longer haul = more risk)
            distance_factor = 0.7 + (distance_miles / max_distance) * 0.8
            # Range: ~0.8 for shortest lane to ~1.5 for longest lane

            # Factor 3: Seasonal effect
            season_factor = seasonal_multiplier[month]

            # Factor 4: Utilization effect
            # Both very low (<55%) and very high (>95%) utilization increase risk
            if utilization_pct < 0.55:
                util_factor = 1.4  # Poorly planned load
            elif utilization_pct > 0.93:
                util_factor = 1.3  # Overloaded
            elif utilization_pct < 0.65:
                util_factor = 1.15  # Below average
            else:
                util_factor = 0.85  # Well-utilized, lower risk

            # Factor 5: Weight extreme effect
            weight_z = abs(weight_lbs - avg_weight) / (avg_weight * weight_var_pct + 1)
            if weight_z > 2.0:
                weight_factor = 1.3  # Extreme weight = higher risk
            elif weight_z > 1.0:
                weight_factor = 1.1  # Moderate deviation
            else:
                weight_factor = 0.9  # Normal range

            # Combined delay probability
            effective_delay_prob = (
                delay_risk
                * carrier_delay_factor
                * distance_factor
                * season_factor
                * util_factor
                * weight_factor
            )

            # Clamp between 2% and 65%
            effective_delay_prob = np.clip(effective_delay_prob, 0.02, 0.65)

            # --- Actual Transit Days ---
            if np.random.random() < effective_delay_prob:
                # Delay occurs — severity also influenced by factors
                if effective_delay_prob > 0.40:
                    delay_days = np.random.choice([2, 3, 4], p=[0.35, 0.40, 0.25])
                elif effective_delay_prob > 0.25:
                    delay_days = np.random.choice([1, 2, 3], p=[0.40, 0.40, 0.20])
                else:
                    delay_days = np.random.choice([1, 2, 3], p=[0.60, 0.30, 0.10])
                actual_transit_days = planned_transit + delay_days
            else:
                if np.random.random() < 0.12:
                    actual_transit_days = max(1, planned_transit - 1)
                else:
                    actual_transit_days = planned_transit

            # --- On-Time Flag ---
            on_time_flag = 1 if actual_transit_days <= planned_transit else 0

            # =============================================================
            # v2: COST-DELAY CORRELATION
            # =============================================================
            # Late shipments tend to also have cost overruns — this creates
            # a realistic correlation between operational issues
            # =============================================================

            carrier_exception_idx = carrier_data['base_exception_index']
            cost_variation_factor = np.random.normal(1.0, 0.03 + carrier_exception_idx * 0.4)

            if on_time_flag == 0:
                # Late shipments have higher cost overruns
                cost_variation_factor += np.random.uniform(0.03, 0.15)

            if np.random.random() < carrier_exception_idx:
                cost_variation_factor += np.random.uniform(0.02, 0.10)

            actual_cost = quoted_cost * cost_variation_factor
            actual_cost = round(max(200, actual_cost), 2)

            # --- Cost Leakage ---
            cost_leakage_amount = round(actual_cost - quoted_cost, 2)

            # =============================================================
            # v2: INVOICE EXCEPTION CORRELATION
            # =============================================================
            # Exception probability increases when shipment is late or
            # has cost overruns — mirrors real-world behavior
            # =============================================================

            base_exception_prob = exception_pct + carrier_exception_idx * 0.2

            # Late shipments are more likely to have invoice issues
            if on_time_flag == 0:
                base_exception_prob += 0.15

            # High cost leakage increases exception probability
            if cost_leakage_amount > quoted_cost * 0.08:
                base_exception_prob += 0.10

            base_exception_prob = min(base_exception_prob, 0.45)
            invoice_exception_flag = 1 if np.random.random() < base_exception_prob else 0

            # --- Build Record (IDENTICAL columns to v1) ---
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
                'planned_transit_days': planned_transit,
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

df = pd.DataFrame(all_shipments)
df = df.sort_values(['shipment_date', 'shipment_id']).reset_index(drop=True)

assert (df['quoted_cost'] > 0).all(), "FAIL: Negative quoted costs"
assert (df['actual_cost'] > 0).all(), "FAIL: Negative actual costs"
print("  PASS: No negative costs")

assert (df['weight_lbs'] > 0).all(), "FAIL: Negative weights"
print("  PASS: No negative weights")

assert (df['utilization_pct'] >= 0.40).all(), "FAIL: Utilization below 40%"
assert (df['utilization_pct'] <= 1.00).all(), "FAIL: Utilization above 100%"
print("  PASS: Utilization within 40%-100% bounds")

assert (df['actual_transit_days'] >= 1).all(), "FAIL: Transit days below 1"
assert (df['actual_transit_days'] <= 15).all(), "FAIL: Unrealistic transit days"
print("  PASS: Transit days within realistic range")

critical_output_cols = ['shipment_id', 'shipment_date', 'lane_id', 'carrier_id',
                        'quoted_cost', 'actual_cost', 'on_time_flag']
for col in critical_output_cols:
    assert df[col].isnull().sum() == 0, f"FAIL: Missing values in {col}"
print("  PASS: No missing values in critical columns")

assert df['shipment_id'].nunique() == len(df), "FAIL: Duplicate shipment IDs"
print("  PASS: No duplicate shipment IDs")

date_range = f"{df['shipment_date'].min().strftime('%Y-%m-%d')} to {df['shipment_date'].max().strftime('%Y-%m-%d')}"
print(f"  INFO: Date range covered: {date_range}")
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

# --- v2: Delay pattern verification ---
print("\n  === v2 DELAY PATTERN VERIFICATION ===")

print("\n  Late rate by carrier (should show clear differences):")
carrier_otif = df.groupby(['carrier_id', 'carrier_name'])['on_time_flag'].agg(['mean', 'count'])
for (cid, cname), row in carrier_otif.iterrows():
    late_rate = (1 - row['mean']) * 100
    print(f"    {cid} ({cname}): {late_rate:.1f}% late ({int(row['count'])} shipments)")

print("\n  Late rate by distance band:")
df['distance_band'] = pd.cut(df['distance_miles'], bins=[0, 500, 1000, 1500, 2000],
                              labels=['Short (<500mi)', 'Medium (500-1000mi)',
                                      'Long (1000-1500mi)', 'Very Long (>1500mi)'])
dist_otif = df.groupby('distance_band', observed=True)['on_time_flag'].mean()
for band, otif in dist_otif.items():
    print(f"    {band}: {(1-otif)*100:.1f}% late")

print("\n  Late rate by season:")
df['season'] = df['month'].map({12: 'Winter', 1: 'Winter', 2: 'Winter',
                                 3: 'Spring', 4: 'Spring', 5: 'Spring',
                                 6: 'Summer', 7: 'Summer', 8: 'Summer',
                                 9: 'Fall', 10: 'Fall', 11: 'Fall'})
season_otif = df.groupby('season')['on_time_flag'].mean()
for season, otif in season_otif.items():
    print(f"    {season}: {(1-otif)*100:.1f}% late")

print("\n  Late rate by utilization band:")
df['util_band'] = pd.cut(df['utilization_pct'], bins=[0, 0.55, 0.65, 0.85, 0.93, 1.0],
                          labels=['Very Low (<55%)', 'Low (55-65%)', 'Normal (65-85%)',
                                  'High (85-93%)', 'Very High (>93%)'])
util_otif = df.groupby('util_band', observed=True)['on_time_flag'].mean()
for band, otif in util_otif.items():
    print(f"    {band}: {(1-otif)*100:.1f}% late")

# Clean up helper columns before export
df.drop(columns=['distance_band', 'season', 'util_band'], inplace=True)

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

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False)

print(f"  Exported: {OUTPUT_FILE}")
print(f"  Rows:     {len(df):,}")
print(f"  Columns:  {len(df.columns)}")
print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")
print()
print("=" * 60)
print("DONE — v2 Dataset generation complete (ML-optimized).")
print("=" * 60)