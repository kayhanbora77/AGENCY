import gc
import numpy as np
import pandas as pd
import lightgbm as lgb
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")


# ============================================
# 1. CONFIGURATION
# ============================================
class CFG:
    seed = 42
    id_col = "id"
    target_col = "y_target"
    weight_col = "weight"
    time_col = "ts_index"
    horizon_col = "horizon"

    # Simplified parameters
    n_lag_features = 5  # Reduced from 8
    feature_sample_size = 200_000  # Reduced from 800k
    cv_valid_window = 192
    cv_train_window = 768  # Reduced from 1536

    # LightGBM with fewer estimators for speed
    lgb_params = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "learning_rate": 0.05,  # Higher LR = faster convergence
        "n_estimators": 500,  # Reduced from 2500
        "num_leaves": 128,  # Reduced from 255
        "max_depth": 8,  # Added depth limit
        "min_child_samples": 100,
        "subsample": 0.8,
        "colsample_bytree": 0.7,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": seed,
        "n_jobs": -1,
        "verbose": -1,
    }


# ============================================
# 2. HELPER FUNCTIONS
# ============================================


def reduce_memory_usage(df):
    """Optimize memory usage by downcasting numeric columns"""
    start_mem = df.memory_usage(deep=True).sum() / 1024**2

    for col in df.columns:
        col_type = df[col].dtype

        if col_type == "object" or pd.api.types.is_datetime64_any_dtype(col_type):
            continue

        c_min, c_max = df[col].min(), df[col].max()

        if pd.api.types.is_integer_dtype(col_type):
            if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                df[col] = df[col].astype(np.int8)
            elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                df[col] = df[col].astype(np.int16)
        else:
            if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                df[col] = df[col].astype(np.float32)

    end_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Memory: {start_mem:.0f}MB → {end_mem:.0f}MB")
    return df


def weighted_rmse(y_true, y_pred, w):
    """Competition metric"""
    y_true, y_pred, w = map(np.asarray, [y_true, y_pred, w])
    denom = np.sum(w * np.square(y_true))
    if denom <= 0:
        return 0.0
    ratio = np.sum(w * np.square(y_true - y_pred)) / denom
    return float(np.sqrt(max(0, 1 - ratio)))


def create_lag_features(df, key_cols, feature_cols, n_lags=2):
    """Create simple lag features from past observations"""
    print("Creating lag features...")

    # Sort by key and time
    df = df.sort_values(key_cols + [CFG.time_col]).reset_index(drop=True)

    # Group by entity
    grouped = df.groupby(key_cols)

    for col in feature_cols:
        for lag in range(1, n_lags + 1):
            df[f"{col}_lag{lag}"] = grouped[col].shift(lag).astype(np.float32)

        # Add difference feature
        df[f"{col}_diff1"] = (df[col] - df[f"{col}_lag1"]).astype(np.float32)

    return df.fillna(0)


def encode_categories(df, cat_cols):
    """Simple label encoding for categorical columns"""
    for col in cat_cols:
        df[col] = df[col].astype("category").cat.codes.astype(np.int16)
    return df


# ============================================
# 3. LOAD DATA
# ============================================
print("=" * 50)
print("LOADING DATA")
print("=" * 50)

# Find files
input_root = Path("/kaggle/input/competitions/ts-forecasting")
train_path = next(input_root.rglob("train.parquet"), None)
test_path = next(input_root.rglob("test.parquet"), None)

if train_path is None:
    # Fallback for local testing
    train_path = "train.parquet"
    test_path = "test.parquet"

print(f"Loading {train_path}...")
train = pd.read_parquet(train_path)
test = pd.read_parquet(test_path)

print(f"Train shape: {train.shape}")
print(f"Test shape: {test.shape}")

# Reduce memory
train = reduce_memory_usage(train)
test = reduce_memory_usage(test)

# ============================================
# 4. SIMPLE FEATURE ENGINEERING
# ============================================
print("\n" + "=" * 50)
print("FEATURE ENGINEERING")
print("=" * 50)

# Identify columns
feature_cols = [col for col in train.columns if col.startswith("feature_")]
cat_cols = ["code", "sub_code", "sub_category"]
print(f"Features: {len(feature_cols)}")
print(f"Categorical: {cat_cols}")

# Quick feature selection (top by variance)
print("Selecting top features...")
feature_variance = train[feature_cols].var().sort_values(ascending=False)
top_features = feature_variance.head(CFG.n_lag_features).index.tolist()
print(f"Selected features: {top_features}")

# Encode categorical variables
train = encode_categories(train, cat_cols)
test = encode_categories(test, cat_cols)

# Create lag features (only on training + test combined for consistency)
key_cols = cat_cols + [CFG.horizon_col]
combined = pd.concat([train, test], axis=0, ignore_index=True)
combined = create_lag_features(combined, key_cols, top_features, n_lags=2)

# Split back
train_len = len(train)
train = combined.iloc[:train_len].copy()
test = combined.iloc[train_len:].copy()

# Add simple aggregations
for df in [train, test]:
    # Row statistics
    df["feature_mean"] = df[feature_cols].mean(axis=1).astype(np.float32)
    df["feature_std"] = df[feature_cols].std(axis=1).fillna(0).astype(np.float32)
    df["feature_missing"] = df[feature_cols].isna().sum(axis=1).astype(np.int16)

    # Time scaling
    df["time_scaled"] = (df[CFG.time_col] / df[CFG.time_col].max()).astype(np.float32)

# Define final feature set
lag_features = (
    [f"{f}_lag1" for f in top_features]
    + [f"{f}_lag2" for f in top_features]
    + [f"{f}_diff1" for f in top_features]
)

model_features = (
    top_features
    + lag_features
    + ["feature_mean", "feature_std", "feature_missing", "time_scaled"]
    + cat_cols
    + [CFG.horizon_col]
)

# Remove any missing columns
model_features = [f for f in model_features if f in train.columns]

print(f"Final features: {len(model_features)}")

# Clean memory
gc.collect()

# ============================================
# 5. TRAINING WITH SIMPLE VALIDATION
# ============================================
print("\n" + "=" * 50)
print("TRAINING")
print("=" * 50)

# Create a simple time-based validation split
unique_times = sorted(train[CFG.time_col].unique())
split_idx = int(len(unique_times) * 0.8)  # 80% train, 20% validation
valid_start_time = unique_times[split_idx]

train_mask = train[CFG.time_col] < valid_start_time
valid_mask = train[CFG.time_col] >= valid_start_time

X_train = train.loc[train_mask, model_features]
y_train = train.loc[train_mask, CFG.target_col]
w_train = train.loc[train_mask, CFG.weight_col]

X_valid = train.loc[valid_mask, model_features]
y_valid = train.loc[valid_mask, CFG.target_col]
w_valid = train.loc[valid_mask, CFG.weight_col]

print(f"Train: {len(X_train):,} rows")
print(f"Valid: {len(X_valid):,} rows")
print(f"Validation start time: {valid_start_time}")

# Train model
print("\nTraining LightGBM...")
model = lgb.LGBMRegressor(**CFG.lgb_params)

model.fit(
    X_train,
    y_train,
    sample_weight=w_train,
    eval_set=[(X_valid, y_valid)],
    eval_sample_weight=[w_valid],
    eval_metric="rmse",
    callbacks=[
        lgb.early_stopping(stopping_rounds=50, verbose=True),
        lgb.log_evaluation(period=100),
    ],
)

print(f"Best iteration: {model.best_iteration_}")

# Validate
y_pred = model.predict(X_valid)
score = weighted_rmse(y_valid, y_pred, w_valid)
print(f"\nValidation Weighted RMSE: {score:.6f}")

# Per-horizon scores
print("\nScores by horizon:")
for h in sorted(X_valid[CFG.horizon_col].unique()):
    mask = X_valid[CFG.horizon_col] == h
    if mask.sum() > 0:
        h_score = weighted_rmse(y_valid[mask], y_pred[mask], w_valid[mask])
        print(f"  Horizon {h}: {h_score:.6f} ({mask.sum():,} rows)")

# ============================================
# 6. PREDICT AND SUBMIT
# ============================================
print("\n" + "=" * 50)
print("CREATING SUBMISSION")
print("=" * 50)

# Predict on test
X_test = test[model_features]
test_pred = model.predict(X_test)

# Simple clipping using training quantiles
print("Clipping predictions...")
clip_lower = train[CFG.target_col].quantile(0.001)
clip_upper = train[CFG.target_col].quantile(0.999)
test_pred = np.clip(test_pred, clip_lower, clip_upper)

# Create submission
submission = pd.DataFrame(
    {CFG.id_col: test[CFG.id_col].values, "prediction": test_pred.astype(np.float64)}
)

# Validate
assert submission[CFG.id_col].is_unique
assert len(submission) == len(test)
print(f"Submission shape: {submission.shape}")

# Save
submission.to_csv("submission.csv", index=False)
print("Saved: submission.csv")

# Show sample
print("\nSubmission preview:")
print(submission.head(10))

print("\nPrediction statistics:")
print(submission["prediction"].describe())

# ============================================
# 7. QUICK FEATURE IMPORTANCE
# ============================================
print("\n" + "=" * 50)
print("TOP FEATURES")
print("=" * 50)

importance_df = (
    pd.DataFrame({"feature": model_features, "importance": model.feature_importances_})
    .sort_values("importance", ascending=False)
    .head(15)
)

print(importance_df.to_string(index=False))

print("\n" + "=" * 50)
print("DONE!")
print("=" * 50)
