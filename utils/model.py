import numpy as np
import pandas as pd
import neurokit2 as nk

from numpy import trapz
from scipy.signal import butter, filtfilt

def combine_data(measurements, patients):

    measurements_df = pd.DataFrame(measurements)
    patients_df     = pd.DataFrame(patients)

    merged = pd.merge(
        measurements_df,
        patients_df,
        on="mobile",
        how="inner"
    )

    combined_data_add = [] ; combined_data_onset = []

    for idx, row in merged.iterrows():

        # start_test_ts > start_ts always
        # start_ts indicates start time of finding FHR
        # start_test_ts indicates when FHR has been found
        # earliest_measurement = row["date_joined"]

        # if row["measurement_date"] < earliest_measurement:
        #     print(f"SKIPPED {idx}: {row['measurement_date']} | {earliest_measurement}")
        #     continue

        if row["delivery_type"] == "c-section":
            print(f"SKIPPED {idx}: C-section")
            continue

        record = {
            "_id"               : row["_id"],               # filt
            "mobile"            : row["mobile"],            # filt/unified
            "uc"                : row["uc"],                # filt
            "fhr"               : row["fhr"],               # filt
            "fmov"              : row["fmov"],              # filt
            "gest_age"          : row["gest_age"],          # filt
            "measurement_date"  : row["measurement_date"],  # filt
            "start_test_ts"     : row["start_test_ts"],     # filt
            "age"               : row["age"] if pd.notna(row['age']) else None, # unified
            "bmi"               : row["bmi"] if pd.notna(row['bmi']) else None, # unified
            "had_pregnancy"     : row["had_pregnancy"],     # unified
            "had_preterm"       : row["had_preterm"],       # unified
            "had_surgery"       : row["had_surgery"],       # unified
            "gdm"               : row["gdm"],               # unified
            "pih"               : row["pih"],               # unified
        }

        # filt
        if row["add"] is not None:
            record["add"] = row["add"]
            combined_data_add.append(record)

        # unified (nullable)
        if row["onset"] is not None:
            record["onset"] = row["onset"]
            combined_data_onset.append(record)

    return combined_data_add, combined_data_onset

WINDOW_SIZE_SECONDS = 10 * 60

def _percentile_bt(series, pct=10):

    h, bins = np.histogram(series, bins=np.arange(series.min(), series.max() + 2))
    cdf = np.cumsum(h) / h.sum()
    idx = np.searchsorted(cdf, pct / 100)

    return bins[idx]

def _bt_series(uc_vals, fs=1):

    win = 10 * 60 * fs
    half = win // 2
    bt = np.zeros_like(uc_vals, float)
    for i in range(len(uc_vals)):
        lo, hi = max(0, i - half), min(len(uc_vals), i + half)
        bt[i] = _percentile_bt(uc_vals[lo:hi], 10)

    return bt

def compute_fhr_baseline(fhr, fs=1, cutoff=0.005):

    def _lp(sig):
        b, a = butter(4, cutoff / (fs / 2), btype='low')
        padlen = 3 * max(len(a), len(b))
        if len(sig) <= padlen:
            return sig
        return filtfilt(b, a, sig)

    baseline = _lp(fhr)

    for i in range(3):
        upper = baseline + (20 - 5 * i)
        lower = baseline - 20
        clipped = np.clip(fhr, lower, upper)
        baseline = _lp(clipped)

    return baseline

def extract_features(data):

    extracted = []

    for row in data:

        # uc = np.array(row["uc"], dtype=np.float64)
        #
        # # Total AUC
        # total_auc = float(trapz(uc, dx=1))
        #
        # # Baseline Tone
        # bt_series       = _bt_series(uc, fs=1)
        # baseline_tone   = float(np.median(bt_series))
        #
        # # Sample Entropy
        # sample_ent = float(nk.entropy_sample(uc, dimension=2, r=0.2 * np.std(uc))[0])

        record = {
            "_id"               : row["_id"],
            "mobile"            : row["mobile"],
            "uc"                : row["uc"],
            "fhr"               : row["fhr"],
            "fmov"              : row["fmov"],
            "gest_age"          : row["gest_age"],
            "measurement_date"  : row["measurement_date"],
            "start_test_ts"     : row["start_test_ts"],
            "age"               : row["age"],
            "bmi"               : row["bmi"],
            "had_pregnancy"     : row["had_pregnancy"],
            "had_preterm"       : row["had_preterm"],
            "had_surgery"       : row["had_surgery"],
            "gdm"               : row["gdm"],
            "pih"               : row["pih"],
            "add"               : row["add"],
            # "total_auc"         : total_auc,
            # "baseline_tone"     : baseline_tone,
            # "sample_entropy"    : sample_ent
        }

        onset = row.get("onset")
        if onset is not None:
            record["onset"] = onset

        extracted.append(record)

    return extracted

def bmi_choose_weight_kg(height_cm, weight_val):
    """
    Resolve 斤 vs kg:
      - If weight > 110 → treat as 斤 (kg = x * 0.5)
      - Else compute BMI for both kg and 斤 and pick the one within [15, 45].
        If both plausible or both implausible, default to kg when <= 110.
    """

    def _try_float(x):
        try:
            return float(str(x).strip())
        except Exception as e:
            print(e)
            return None

    h_cm = pd.to_numeric(height_cm, errors="coerce")
    w = _try_float(weight_val)
    if pd.isna(h_cm) or h_cm <= 0 or w is None:
        return None

    h_m = h_cm / 100.0
    kg_if_kg = w
    kg_if_jin = w * 0.5

    def _bmi(kg):
        return (kg / (h_m ** 2)) if (kg and h_m > 0) else None

    b1 = _bmi(kg_if_kg)
    b2 = _bmi(kg_if_jin)

    def plausible(b) -> bool:
        return (b is not None) and (15.0 <= b <= 45.0)

    if w > 110:
        return round(b2, 1) if b2 is not None else None
    if plausible(b1) and not plausible(b2):
        return round(b1, 1)
    if plausible(b2) and not plausible(b1):
        return round(b2, 1)
    return round(b1, 1) if b1 is not None else None