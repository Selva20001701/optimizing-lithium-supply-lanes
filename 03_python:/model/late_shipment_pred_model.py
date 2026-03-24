"""
=============================================================================
LAYER 3: Late Shipment Risk Prediction Model (Enhanced v2)
Project: Optimizing Lithium Supply Lanes
         Truck Freight Cost, Service, and Carrier Performance
=============================================================================

This script builds a predictive model that estimates the probability of a
shipment arriving late, using pre-departure features and engineered
historical aggregates.

Models: Logistic Regression, Random Forest, XGBoost
Explainability: SHAP (TreeExplainer on best tree-based model)
Output: CSVs + saved model for Streamlit deployment

Author: [Your Name]
Date: March 2026
=============================================================================
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    f1_score, roc_auc_score, confusion_matrix
)
import joblib
import warnings
warnings.filterwarnings('ignore')

# XGBoost
try:
    from xgboost import XGBClassifier
    XGBOOST_AVAILABLE = True
    print("XGBoost library loaded successfully.")
except ImportError:
    XGBOOST_AVAILABLE = False
    print("WARNING: XGBoost not installed. Run 'pip3 install xgboost'")
    print("Continuing with Logistic Regression and Random Forest only...")

# SHAP
try:
    import shap
    SHAP_AVAILABLE = True
    print("SHAP library loaded successfully.")
except ImportError:
    SHAP_AVAILABLE = False
    print("WARNING: SHAP not installed. Run 'pip3 install shap'")
    print("Continuing without SHAP analysis...")

print()


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
print(f"PROJECT_ROOT:{PROJECT_ROOT}")
OUTPUT_DIR = PROJECT_ROOT / "04_outputs:"
MODEL_DIR = PROJECT_ROOT / "04_outputs:" / "model_artifacts"
INPUT_FILE = OUTPUT_DIR / "synthetic_shipments_24m.csv"
MODEL_OUTPUT_FILE = OUTPUT_DIR / "shipments_with_late_risk.csv"
FEATURE_IMPORTANCE_FILE = OUTPUT_DIR / "feature_importance.csv"
MODEL_SUMMARY_FILE = OUTPUT_DIR / "model_evaluation_summary.csv"
CONFUSION_MATRIX_FILE = OUTPUT_DIR / "confusion_matrices.csv"
SHAP_SUMMARY_FILE = OUTPUT_DIR / "shap_feature_summary.csv"
SHAP_SHIPMENT_FILE = OUTPUT_DIR / "shap_per_shipment_drivers.csv"

# Model artifacts for Streamlit deployment
SAVED_MODEL_FILE = MODEL_DIR / "best_model.pkl"
SAVED_ENCODERS_FILE = MODEL_DIR / "label_encoders.pkl"
SAVED_FEATURE_LIST_FILE = MODEL_DIR / "feature_columns.pkl"
SAVED_FILL_VALUES_FILE = MODEL_DIR / "fill_values.pkl"

RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)


# =============================================================================
# SECTION 1: LOAD DATA
# =============================================================================

print("=" * 60)
print("SECTION 1: Loading shipment data...")
print("=" * 60)

df = pd.read_csv(INPUT_FILE)
df['shipment_date'] = pd.to_datetime(df['shipment_date'])

print(f"  Loaded: {len(df):,} shipments")
print(f"  Columns: {len(df.columns)}")
print(f"  Late shipments: {(df['on_time_flag'] == 0).sum():,} ({(df['on_time_flag'] == 0).mean():.2%})")
print(f"  On-time shipments: {(df['on_time_flag'] == 1).sum():,} ({(df['on_time_flag'] == 1).mean():.2%})")
print()


# =============================================================================
# SECTION 2: ADVANCED FEATURE ENGINEERING
# =============================================================================

print("=" * 60)
print("SECTION 2: Engineering advanced features...")
print("=" * 60)

df = df.sort_values('shipment_date').reset_index(drop=True)

# --- 2A: Historical carrier performance aggregates ---
print("  Building carrier historical aggregates...")

df['carrier_hist_otif'] = (
    df.groupby('carrier_id')['on_time_flag']
    .transform(lambda x: x.expanding().mean().shift(1))
)

df['carrier_hist_exception_rate'] = (
    df.groupby('carrier_id')['invoice_exception_flag']
    .transform(lambda x: x.expanding().mean().shift(1))
)

# --- 2B: Historical lane performance aggregates ---
print("  Building lane historical aggregates...")

df['lane_hist_otif'] = (
    df.groupby('lane_id')['on_time_flag']
    .transform(lambda x: x.expanding().mean().shift(1))
)

df['transit_variance'] = df['actual_transit_days'] - df['planned_transit_days']
df['lane_hist_avg_delay'] = (
    df.groupby('lane_id')['transit_variance']
    .transform(lambda x: x.expanding().mean().shift(1))
)

# --- 2C: Historical lane-carrier combo performance ---
print("  Building lane-carrier combo aggregates...")

df['lane_carrier_hist_otif'] = (
    df.groupby(['lane_id', 'carrier_id'])['on_time_flag']
    .transform(lambda x: x.expanding().mean().shift(1))
)

# --- 2D: Interaction features ---
print("  Building interaction features...")

df['cost_per_mile_quoted'] = df['quoted_cost'] / df['distance_miles'].replace(0, np.nan)

carrier_risk_map = df.groupby('carrier_id')['on_time_flag'].mean().to_dict()
df['carrier_avg_otif_static'] = df['carrier_id'].map(carrier_risk_map)
df['distance_x_carrier_risk'] = df['distance_miles'] * (1 - df['carrier_avg_otif_static'])

df['weight_util_ratio'] = df['weight_lbs'] / df['utilization_pct'].replace(0, np.nan)

# --- 2E: Fill NaN from first-row shift ---
print("  Filling missing values from initial records...")

fill_values = {
    'carrier_hist_otif': df['on_time_flag'].mean(),
    'carrier_hist_exception_rate': df['invoice_exception_flag'].mean(),
    'lane_hist_otif': df['on_time_flag'].mean(),
    'lane_hist_avg_delay': 0,
    'lane_carrier_hist_otif': df['on_time_flag'].mean(),
    'cost_per_mile_quoted': df['cost_per_mile_quoted'].median(),
    'weight_util_ratio': df['weight_util_ratio'].median()
}
df.fillna(fill_values, inplace=True)

df.drop(columns=['transit_variance', 'carrier_avg_otif_static'], inplace=True)

print(f"  New features created: 8")
print("    - carrier_hist_otif")
print("    - carrier_hist_exception_rate")
print("    - lane_hist_otif")
print("    - lane_hist_avg_delay")
print("    - lane_carrier_hist_otif")
print("    - cost_per_mile_quoted")
print("    - distance_x_carrier_risk")
print("    - weight_util_ratio")
print()


# =============================================================================
# SECTION 3: SELECT FEATURES
# =============================================================================

print("=" * 60)
print("SECTION 3: Selecting features...")
print("=" * 60)

base_features = [
    'lane_id', 'carrier_id', 'distance_miles', 'weight_lbs',
    'quoted_cost', 'planned_transit_days', 'utilization_pct',
    'volume_band', 'service_sensitivity', 'base_risk_band',
    'strategic_priority', 'priority_flag', 'month', 'quarter'
]

engineered_features = [
    'carrier_hist_otif', 'carrier_hist_exception_rate',
    'lane_hist_otif', 'lane_hist_avg_delay',
    'lane_carrier_hist_otif', 'cost_per_mile_quoted',
    'distance_x_carrier_risk', 'weight_util_ratio'
]

feature_columns = base_features + engineered_features
target_column = 'on_time_flag'

print(f"  Total features: {len(feature_columns)}")
print(f"    Base features: {len(base_features)}")
print(f"    Engineered features: {len(engineered_features)}")
print()


# =============================================================================
# SECTION 4: ENCODE CATEGORICAL FEATURES
# =============================================================================

print("=" * 60)
print("SECTION 4: Encoding categorical features...")
print("=" * 60)

categorical_cols = [
    'lane_id', 'carrier_id', 'volume_band',
    'service_sensitivity', 'base_risk_band',
    'strategic_priority', 'priority_flag'
]

model_df = df[feature_columns + [target_column]].copy()

label_encoders = {}
for col in categorical_cols:
    le = LabelEncoder()
    model_df[col] = le.fit_transform(model_df[col])
    label_encoders[col] = le
    print(f"  Encoded {col}: {len(le.classes_)} categories → {list(le.classes_)}")

print()


# =============================================================================
# SECTION 5: SPLIT DATA — TRAIN / TEST
# =============================================================================

print("=" * 60)
print("SECTION 5: Splitting data into train/test sets...")
print("=" * 60)

X = model_df[feature_columns]
y = model_df[target_column]

y_late = (y == 0).astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y_late, test_size=0.20, random_state=RANDOM_SEED, stratify=y_late
)

print(f"  Training set: {len(X_train):,} shipments")
print(f"  Test set:     {len(X_test):,} shipments")
print(f"  Late rate (train): {y_train.mean():.2%}")
print(f"  Late rate (test):  {y_test.mean():.2%}")
print()


# =============================================================================
# SECTION 6: TRAIN ALL MODELS
# =============================================================================

print("=" * 60)
print("SECTION 6: Training all models...")
print("=" * 60)

# --- 6A: Logistic Regression ---
print("\n  Training Logistic Regression...")
lr_model = LogisticRegression(
    max_iter=1000,
    random_state=RANDOM_SEED,
    class_weight='balanced'
)
lr_model.fit(X_train, y_train)
lr_pred = lr_model.predict(X_test)
lr_prob = lr_model.predict_proba(X_test)[:, 1]
print("  Done.")

# --- 6B: Random Forest ---
print("  Training Random Forest...")
rf_model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    min_samples_split=10,
    min_samples_leaf=5,
    random_state=RANDOM_SEED,
    class_weight='balanced'
)
rf_model.fit(X_train, y_train)
rf_pred = rf_model.predict(X_test)
rf_prob = rf_model.predict_proba(X_test)[:, 1]
print("  Done.")

# --- 6C: XGBoost ---
if XGBOOST_AVAILABLE:
    print("  Training XGBoost...")
    # Calculate scale_pos_weight for class imbalance
    n_negative = (y_train == 0).sum()
    n_positive = (y_train == 1).sum()
    scale_weight = n_negative / n_positive

    xgb_model = XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        min_child_weight=5,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_weight,
        random_state=RANDOM_SEED,
        eval_metric='logloss',
        use_label_encoder=False
    )
    xgb_model.fit(X_train, y_train)
    xgb_pred = xgb_model.predict(X_test)
    xgb_prob = xgb_model.predict_proba(X_test)[:, 1]
    print("  Done.")

print()


# =============================================================================
# SECTION 7: MODEL COMPARISON TABLE
# =============================================================================

print("=" * 60)
print("SECTION 7: Model comparison...")
print("=" * 60)

def evaluate_model(name, y_true, y_pred, y_prob):
    """Calculate all evaluation metrics for a model."""
    return {
        'Model': name,
        'Accuracy': round(accuracy_score(y_true, y_pred), 4),
        'Precision': round(precision_score(y_true, y_pred), 4),
        'Recall': round(recall_score(y_true, y_pred), 4),
        'F1_Score': round(f1_score(y_true, y_pred), 4),
        'ROC_AUC': round(roc_auc_score(y_true, y_prob), 4)
    }

results = [
    evaluate_model('Logistic Regression', y_test, lr_pred, lr_prob),
    evaluate_model('Random Forest', y_test, rf_pred, rf_prob),
]

if XGBOOST_AVAILABLE:
    results.append(evaluate_model('XGBoost', y_test, xgb_pred, xgb_prob))

comparison = pd.DataFrame(results)

print("\n  ┌─────────────────────────────────────────────────────────────────────┐")
print("  │                    MODEL COMPARISON TABLE                          │")
print("  ├─────────────────────┬──────────┬───────────┬────────┬───────┬──────┤")
print("  │ Model               │ Accuracy │ Precision │ Recall │   F1  │  AUC │")
print("  ├─────────────────────┼──────────┼───────────┼────────┼───────┼──────┤")
for _, row in comparison.iterrows():
    print(f"  │ {row['Model']:<19s} │  {row['Accuracy']:.4f}  │  {row['Precision']:.4f}   │ {row['Recall']:.4f} │{row['F1_Score']:.4f} │{row['ROC_AUC']:.4f}│")
print("  └─────────────────────┴──────────┴───────────┴────────┴───────┴──────┘")
print()


# =============================================================================
# SECTION 8: CONFUSION MATRICES — ALL MODELS
# =============================================================================

print("=" * 60)
print("SECTION 8: Confusion matrices...")
print("=" * 60)

def print_confusion_matrix(name, y_true, y_pred):
    """Print formatted confusion matrix."""
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()
    total = len(y_true)

    print(f"\n  {name}:")
    print(f"  ┌────────────────────┬──────────────────┬──────────────────┐")
    print(f"  │                    │ Predicted OnTime │ Predicted Late   │")
    print(f"  ├────────────────────┼──────────────────┼──────────────────┤")
    print(f"  │ Actually On-Time   │ TN = {tn:<11d} │ FP = {fp:<11d} │")
    print(f"  │ Actually Late      │ FN = {fn:<11d} │ TP = {tp:<11d} │")
    print(f"  └────────────────────┴──────────────────┴──────────────────┘")
    print(f"    True Negative Rate:  {tn/(tn+fp):.2%}  (correctly identified on-time)")
    print(f"    True Positive Rate:  {tp/(tp+fn):.2%}  (correctly caught late shipments)")
    print(f"    False Positive Rate: {fp/(tn+fp):.2%}  (false alarms)")
    print(f"    False Negative Rate: {fn/(tp+fn):.2%}  (missed late shipments)")

    return {'Model': name, 'TN': tn, 'FP': fp, 'FN': fn, 'TP': tp,
            'TNR': round(tn/(tn+fp), 4), 'TPR': round(tp/(tp+fn), 4),
            'FPR': round(fp/(tn+fp), 4), 'FNR': round(fn/(tp+fn), 4)}

cm_results = [
    print_confusion_matrix('Logistic Regression', y_test, lr_pred),
    print_confusion_matrix('Random Forest', y_test, rf_pred),
]

if XGBOOST_AVAILABLE:
    cm_results.append(print_confusion_matrix('XGBoost', y_test, xgb_pred))

cm_df = pd.DataFrame(cm_results)
print()


# =============================================================================
# SECTION 9: AUTO-SELECT BEST MODEL
# =============================================================================

print("=" * 60)
print("SECTION 9: Selecting best model...")
print("=" * 60)

# Select based on ROC AUC (primary) with F1 as tiebreaker
best_row = comparison.sort_values(['ROC_AUC', 'F1_Score'], ascending=False).iloc[0]
best_name = best_row['Model']

# Map name to model object and full predictions
model_map = {
    'Logistic Regression': (lr_model, lr_prob),
    'Random Forest': (rf_model, rf_prob),
}
if XGBOOST_AVAILABLE:
    model_map['XGBoost'] = (xgb_model, xgb_prob)

best_model, best_test_prob = model_map[best_name]

# Generate predictions on full dataset
best_prob_full = best_model.predict_proba(X)[:, 1]

print(f"\n  ╔═══════════════════════════════════════════════╗")
print(f"  ║  BEST MODEL: {best_name:<33s} ║")
print(f"  ║  ROC AUC:    {str(best_row['ROC_AUC']):<33s}  ║")
print(f"  ║  F1 Score:   {str(best_row['F1_Score']):<33s}  ║")
print(f"  ║  Accuracy:   {str(best_row['Accuracy']):<33s}  ║")
print(f"  ╚═══════════════════════════════════════════════╝")
print()


# =============================================================================
# SECTION 10: FEATURE IMPORTANCE
# =============================================================================

print("=" * 60)
print("SECTION 10: Feature importance...")
print("=" * 60)

# Use tree-based model for feature importance (RF or XGBoost)
if best_name == 'XGBoost' and XGBOOST_AVAILABLE:
    importance_model = xgb_model
    importance_source = "XGBoost"
elif best_name == 'Random Forest':
    importance_model = rf_model
    importance_source = "Random Forest"
else:
    # If LR was best, still use RF for feature importance
    importance_model = rf_model
    importance_source = "Random Forest (reference)"

importances = importance_model.feature_importances_
feature_imp = pd.DataFrame({
    'feature': feature_columns,
    'importance': importances,
    'feature_type': ['base' if f in base_features else 'engineered' for f in feature_columns]
}).sort_values('importance', ascending=False)

feature_imp['importance_pct'] = (feature_imp['importance'] * 100).round(2)
feature_imp['rank'] = range(1, len(feature_imp) + 1)

print(f"\n  Feature Importance Ranking (from {importance_source}):")
for _, row in feature_imp.iterrows():
    bar = "█" * int(row['importance_pct'] * 2)
    tag = " ★" if row['feature_type'] == 'engineered' else ""
    print(f"    {row['rank']:2d}. {row['feature']:<30s} {row['importance_pct']:6.2f}%  {bar}{tag}")

print("\n  ★ = engineered feature")
print()


# =============================================================================
# SECTION 11: SHAP EXPLAINABILITY
# =============================================================================

print("=" * 60)
print("SECTION 11: SHAP explainability analysis...")
print("=" * 60)

if SHAP_AVAILABLE:
    # Use a tree-based model for SHAP TreeExplainer
    if best_name in ['Random Forest', 'XGBoost']:
        shap_model = best_model
        shap_source = best_name
    else:
        shap_model = rf_model
        shap_source = "Random Forest (reference)"

    shap_sample_size = min(500, len(X_test))
    X_shap = X_test.iloc[:shap_sample_size]

    print(f"  Running SHAP on {shap_sample_size} test samples using {shap_source}...")

    explainer = shap.TreeExplainer(shap_model)
    shap_values = explainer.shap_values(X_shap)

    if isinstance(shap_values, list):
        shap_late = shap_values[1]
    else:
        shap_late = shap_values

    # --- 11A: Global SHAP summary ---
    shap_global = pd.DataFrame({
        'feature': feature_columns,
        'mean_abs_shap': np.abs(shap_late).mean(axis=0),
        'feature_type': ['base' if f in base_features else 'engineered' for f in feature_columns]
    }).sort_values('mean_abs_shap', ascending=False)

    shap_global['shap_rank'] = range(1, len(shap_global) + 1)
    shap_global['shap_pct'] = (shap_global['mean_abs_shap'] / shap_global['mean_abs_shap'].sum() * 100).round(2)

    print("\n  SHAP Feature Impact Ranking (what drives late risk):")
    for _, row in shap_global.iterrows():
        bar = "█" * int(row['shap_pct'] * 1.5)
        tag = " ★" if row['feature_type'] == 'engineered' else ""
        print(f"    {row['shap_rank']:2d}. {row['feature']:<30s} {row['shap_pct']:6.2f}%  {bar}{tag}")

    print("\n  ★ = engineered feature")

    # --- 11B: Per-shipment SHAP drivers ---
    print("\n  Building per-shipment SHAP driver explanations...")

    shap_df = pd.DataFrame(shap_late, columns=feature_columns, index=X_shap.index)

    per_shipment_drivers = []
    for idx in shap_df.index:
        row_shap = shap_df.loc[idx]
        top3_up = row_shap.nlargest(3)
        top3_down = row_shap.nsmallest(3)

        per_shipment_drivers.append({
            'shipment_index': idx,
            'top_risk_driver_1': top3_up.index[0],
            'top_risk_driver_1_shap': round(top3_up.values[0], 4),
            'top_risk_driver_2': top3_up.index[1],
            'top_risk_driver_2_shap': round(top3_up.values[1], 4),
            'top_risk_driver_3': top3_up.index[2],
            'top_risk_driver_3_shap': round(top3_up.values[2], 4),
            'top_safety_driver_1': top3_down.index[0],
            'top_safety_driver_1_shap': round(top3_down.values[0], 4),
            'top_safety_driver_2': top3_down.index[1],
            'top_safety_driver_2_shap': round(top3_down.values[1], 4),
            'top_safety_driver_3': top3_down.index[2],
            'top_safety_driver_3_shap': round(top3_down.values[2], 4),
        })

    shap_drivers_df = pd.DataFrame(per_shipment_drivers)

    print("\n  Example per-shipment explanations (first 5):")
    for i, row in shap_drivers_df.head(5).iterrows():
        print(f"\n    Shipment at index {row['shipment_index']}:")
        print(f"      Risk INCREASED by: {row['top_risk_driver_1']} (+{row['top_risk_driver_1_shap']}), "
              f"{row['top_risk_driver_2']} (+{row['top_risk_driver_2_shap']}), "
              f"{row['top_risk_driver_3']} (+{row['top_risk_driver_3_shap']})")
        print(f"      Risk DECREASED by: {row['top_safety_driver_1']} ({row['top_safety_driver_1_shap']}), "
              f"{row['top_safety_driver_2']} ({row['top_safety_driver_2_shap']}), "
              f"{row['top_safety_driver_3']} ({row['top_safety_driver_3_shap']})")

    print()

else:
    print("  SHAP skipped — library not available.")
    shap_global = None
    shap_drivers_df = None
    print()


# =============================================================================
# SECTION 12: ADD RISK PROBABILITY TO ALL SHIPMENTS
# =============================================================================

print("=" * 60)
print("SECTION 12: Adding late risk probability to shipments...")
print("=" * 60)

df['late_risk_probability'] = best_prob_full.round(4)

df['late_risk_tier'] = pd.cut(
    df['late_risk_probability'],
    bins=[0, 0.15, 0.30, 0.50, 1.0],
    labels=['Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk'],
    include_lowest=True
)

risk_summary = df['late_risk_tier'].value_counts().sort_index()
print("\n  Late Risk Tier Distribution:")
for tier, count in risk_summary.items():
    pct = count / len(df) * 100
    print(f"    {tier}: {count:,} shipments ({pct:.1f}%)")

print()


# =============================================================================
# SECTION 13: RISK ANALYSIS BY LANE AND CARRIER
# =============================================================================

print("=" * 60)
print("SECTION 13: Risk analysis by lane and carrier...")
print("=" * 60)

print("\n  Average Late Risk by Lane:")
lane_risk = df.groupby('lane_id')['late_risk_probability'].mean().sort_values(ascending=False)
for lane_id, risk in lane_risk.items():
    bar = "█" * int(risk * 50)
    print(f"    {lane_id}: {risk:.4f}  {bar}")

print("\n  Average Late Risk by Carrier:")
carrier_risk = df.groupby(['carrier_id', 'carrier_name'])['late_risk_probability'].mean().sort_values(ascending=False)
for (cid, cname), risk in carrier_risk.items():
    bar = "█" * int(risk * 50)
    print(f"    {cid} ({cname}): {risk:.4f}  {bar}")

print()


# =============================================================================
# SECTION 14: SAVE MODEL ARTIFACTS FOR STREAMLIT
# =============================================================================

print("=" * 60)
print("SECTION 14: Saving model artifacts for deployment...")
print("=" * 60)

MODEL_DIR.mkdir(parents=True, exist_ok=True)

# Save the best model
joblib.dump(best_model, SAVED_MODEL_FILE)
print(f"  Saved best model ({best_name}): {SAVED_MODEL_FILE}")

# Save label encoders (needed to transform new inputs)
joblib.dump(label_encoders, SAVED_ENCODERS_FILE)
print(f"  Saved label encoders: {SAVED_ENCODERS_FILE}")

# Save feature column list (needed for input alignment)
joblib.dump(feature_columns, SAVED_FEATURE_LIST_FILE)
print(f"  Saved feature columns: {SAVED_FEATURE_LIST_FILE}")

# Save fill values (needed for missing value handling)
joblib.dump(fill_values, SAVED_FILL_VALUES_FILE)
print(f"  Saved fill values: {SAVED_FILL_VALUES_FILE}")

print()


# =============================================================================
# SECTION 15: EXPORT ALL CSV RESULTS
# =============================================================================

print("=" * 60)
print("SECTION 15: Exporting CSV results...")
print("=" * 60)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Shipments with late risk
df.to_csv(MODEL_OUTPUT_FILE, index=False)
print(f"  Exported shipments with risk: {MODEL_OUTPUT_FILE}")
print(f"    Rows: {len(df):,}")

# 2. Feature importance
feature_imp.to_csv(FEATURE_IMPORTANCE_FILE, index=False)
print(f"  Exported feature importance: {FEATURE_IMPORTANCE_FILE}")

# 3. Model comparison
comparison.to_csv(MODEL_SUMMARY_FILE, index=False)
print(f"  Exported model comparison: {MODEL_SUMMARY_FILE}")

# 4. Confusion matrices
cm_df.to_csv(CONFUSION_MATRIX_FILE, index=False)
print(f"  Exported confusion matrices: {CONFUSION_MATRIX_FILE}")

# 5. SHAP outputs
if SHAP_AVAILABLE and shap_global is not None:
    shap_global.to_csv(SHAP_SUMMARY_FILE, index=False)
    print(f"  Exported SHAP summary: {SHAP_SUMMARY_FILE}")

    shap_drivers_df.to_csv(SHAP_SHIPMENT_FILE, index=False)
    print(f"  Exported SHAP per-shipment drivers: {SHAP_SHIPMENT_FILE}")

print()


# =============================================================================
# FINAL SUMMARY
# =============================================================================

print("=" * 60)
print("LAYER 3 COMPLETE — FINAL SUMMARY")
print("=" * 60)
print(f"""
  Project:         Optimizing Lithium Supply Lanes
  Models trained:  {'3 (LR, RF, XGBoost)' if XGBOOST_AVAILABLE else '2 (LR, RF)'}
  Best model:      {best_name}
  ROC AUC:         {best_row['ROC_AUC']}
  F1 Score:        {best_row['F1_Score']}
  Features:        {len(feature_columns)} ({len(base_features)} base + {len(engineered_features)} engineered)
  SHAP:            {'Yes' if SHAP_AVAILABLE else 'No'}
  
  Output files:
    - {MODEL_OUTPUT_FILE.name}
    - {FEATURE_IMPORTANCE_FILE.name}
    - {MODEL_SUMMARY_FILE.name}
    - {CONFUSION_MATRIX_FILE.name}
    {'- ' + SHAP_SUMMARY_FILE.name if SHAP_AVAILABLE else ''}
    {'- ' + SHAP_SHIPMENT_FILE.name if SHAP_AVAILABLE else ''}
  
  Model artifacts (for Streamlit):
    - {SAVED_MODEL_FILE.name}
    - {SAVED_ENCODERS_FILE.name}
    - {SAVED_FEATURE_LIST_FILE.name}
    - {SAVED_FILL_VALUES_FILE.name}
  
  Ready for Streamlit deployment.
""")
print("=" * 60)