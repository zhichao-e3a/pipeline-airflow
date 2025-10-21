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
        earliest_measurement = row["date_joined"]

        if row["measurement_date"] < earliest_measurement:
            continue

        if row["delivery_type"] == "c-section":
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
            "age"               : row["age"],               # unified
            "bmi"               : row["bmi"],               # unified
            "had_pregnancy"     : row["had_pregnancy"],     # unified
            "had_preterm"       : row["had_preterm"],       # unified
            "had_surgery"       : row["had_surgery"],       # unified
            "gdm"               : row["gdm"],               # unified
            "pih"               : row["pih"],               # unified
        }

        # unified (nullable)
        if row["add"] is not None:
            record["add"] = row["add"]
            combined_data_add.append(record)

        # unified (nullable/empty)
        if row["onset"] is not None and row["onset"]:
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

        uc = np.array(row["uc"], dtype=np.float64)

        # Total AUC
        total_auc = float(trapz(uc, dx=1))

        # Baseline Tone
        bt_series       = _bt_series(uc, fs=1)
        baseline_tone   = float(np.median(bt_series))

        # Sample Entropy
        sample_ent = float(nk.entropy_sample(uc, dimension=2, r=0.2 * np.std(uc))[0])

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
            "total_auc"         : total_auc,
            "baseline_tone"     : baseline_tone,
            "sample_entropy"    : sample_ent
        }

        onset = row.get("onset")
        if onset is not None:
            record["onset"] = onset

        extracted.append(record)

    return extracted