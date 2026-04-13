"""
Yield prediction model: correlation analysis and supervised ML for Missouri corn yield.

Uses NDVI, weather (precipitation, temperature), and lagged features to predict
bushels/acre. Designed to integrate with the live pipeline (NOAA + USDA + Earth Engine)
or run standalone on pre-fetched DataFrames.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import cross_val_score, LeaveOneOut
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, root_mean_squared_error
import joblib
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def build_features(ndvi_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge NDVI and weather into a per-year feature matrix.

    ndvi_df: columns ['year', 'date', 'NDVI']  (daily or monthly)
    weather_df: columns ['year', 'date', 'PRCP', 'TMAX', 'TMIN']

    Returns a DataFrame with one row per year and columns:
        ndvi_peak, ndvi_mean_growing, ndvi_july, ndvi_june,
        prcp_may_aug, prcp_annual, gdd_may_aug,
        drought_flag (fraction of growing-season weeks below 10mm precip),
        tmax_july_mean
    """
    features = []

    for year in sorted(ndvi_df['year'].unique()):
        ny = ndvi_df[ndvi_df['year'] == year].copy()
        wy = weather_df[weather_df['year'] == year].copy()

        ny['month'] = pd.to_datetime(ny['date']).dt.month
        wy['month'] = pd.to_datetime(wy['date']).dt.month

        # NDVI features
        growing = ny[ny['month'].between(4, 9)]
        ndvi_peak = ny['NDVI'].max()
        ndvi_mean = growing['NDVI'].mean() if not growing.empty else np.nan
        ndvi_july = ny[ny['month'] == 7]['NDVI'].mean()
        ndvi_june = ny[ny['month'] == 6]['NDVI'].mean()

        # Weather features
        growing_w = wy[wy['month'].between(5, 8)]
        prcp_may_aug = growing_w['PRCP'].sum() if 'PRCP' in growing_w else np.nan
        prcp_annual = wy['PRCP'].sum() if 'PRCP' in wy else np.nan

        # Growing Degree Days (base 10C): GDD = max(0, (TMAX+TMIN)/2 - 10)
        if 'TMAX' in growing_w and 'TMIN' in growing_w:
            tmean = (growing_w['TMAX'] + growing_w['TMIN']) / 2
            gdd = np.maximum(0, tmean - 10).sum()
        else:
            gdd = np.nan

        # Drought flag: fraction of May-Aug weeks where weekly precip < 10mm
        if 'PRCP' in growing_w and not growing_w.empty:
            weekly = growing_w.set_index('date')['PRCP'].resample('W').sum()
            drought_flag = (weekly < 10).mean()
        else:
            drought_flag = np.nan

        tmax_july = wy[wy['month'] == 7]['TMAX'].mean() if 'TMAX' in wy.columns else np.nan

        features.append({
            'year': year,
            'ndvi_peak': ndvi_peak,
            'ndvi_mean_growing': ndvi_mean,
            'ndvi_july': ndvi_july,
            'ndvi_june': ndvi_june,
            'prcp_may_aug': prcp_may_aug,
            'prcp_annual': prcp_annual,
            'gdd_may_aug': gdd,
            'drought_flag': drought_flag,
            'tmax_july_mean': tmax_july,
        })

    return pd.DataFrame(features).set_index('year')


# ---------------------------------------------------------------------------
# Correlation analysis
# ---------------------------------------------------------------------------

def compute_correlations(
    ndvi_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    yield_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute Pearson correlation matrix between NDVI, weather, and yield.

    yield_df: columns ['year', 'yield_bu_acre']

    Returns: correlation matrix DataFrame (features x features, including yield).
    """
    feat = build_features(ndvi_df, weather_df)
    y = yield_df.set_index('year')['yield_bu_acre']
    combined = feat.join(y, how='inner')

    if combined.empty:
        logger.warning("No overlapping years between features and yield data.")
        return pd.DataFrame()

    corr = combined.corr(method='pearson').round(3)
    logger.info("Correlation matrix computed for %d years.", len(combined))
    return corr


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def train_model(
    features: pd.DataFrame,
    targets: pd.Series,
    model_type: str = 'gbr',
    cv: str = 'loo',
) -> dict:
    """
    Train a yield prediction model with cross-validation.

    Args:
        features: per-year feature matrix (from build_features or external)
        targets: yield in bu/acre, indexed by year
        model_type: 'ridge' or 'gbr' (GradientBoostingRegressor)
        cv: 'loo' (LeaveOneOut) or int for KFold

    Returns dict with:
        model: fitted sklearn Pipeline
        cv_rmse: cross-validated RMSE
        cv_r2: cross-validated R²
        feature_importance: Series (GBR only; None for Ridge)
        train_r2: R² on full training set
    """
    # Align
    idx = features.index.intersection(targets.index)
    X = features.loc[idx].copy()
    y = targets.loc[idx].copy()

    X = X.fillna(X.median())

    if len(X) < 4:
        raise ValueError(f"Need at least 4 samples; got {len(X)}.")

    # Build pipeline
    if model_type == 'ridge':
        estimator = Ridge(alpha=1.0)
        pipe = Pipeline([('scaler', StandardScaler()), ('model', estimator)])
    else:
        estimator = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            subsample=0.8,
            random_state=42,
        )
        pipe = Pipeline([('scaler', StandardScaler()), ('model', estimator)])

    # Cross-validation
    # For LOO: compute R² on the full OOF prediction array (per-fold R² is undefined with 1 sample)
    from sklearn.model_selection import cross_val_predict
    cv_strategy = LeaveOneOut() if cv == 'loo' else int(cv)
    y_oof = cross_val_predict(pipe, X, y, cv=cv_strategy)
    cv_rmse = float(root_mean_squared_error(y, y_oof))
    cv_r2 = float(r2_score(y, y_oof))

    # Fit on full data
    pipe.fit(X, y)
    y_pred = pipe.predict(X)
    train_r2 = r2_score(y, y_pred)
    train_rmse = float(root_mean_squared_error(y, y_pred))

    # Feature importance (GBR only)
    feat_imp = None
    if model_type == 'gbr':
        raw_imp = pipe.named_steps['model'].feature_importances_
        feat_imp = pd.Series(raw_imp, index=X.columns).sort_values(ascending=False)

    logger.info(
        "Model: %s | CV RMSE: %.2f bu/acre | CV R²: %.3f | Train R²: %.3f",
        model_type.upper(), cv_rmse, cv_r2, train_r2
    )

    return {
        'model': pipe,
        'feature_names': list(X.columns),
        'cv_rmse': cv_rmse,
        'cv_r2': cv_r2,
        'train_rmse': train_rmse,
        'train_r2': train_r2,
        'feature_importance': feat_imp,
        'y_pred_train': pd.Series(y_pred, index=idx),
        'y_true_train': y,
    }


def save_model(result: dict, path: str) -> None:
    """Persist trained pipeline to disk."""
    joblib.dump(result['model'], path)
    logger.info("Model saved to %s", path)


def load_model(path: str):
    """Load a previously saved pipeline."""
    return joblib.load(path)


def predict(model, features: pd.DataFrame) -> pd.Series:
    """Run inference on new feature rows."""
    X = features.fillna(features.median())
    preds = model.predict(X)
    return pd.Series(preds, index=features.index, name='yield_pred_bu_acre')
