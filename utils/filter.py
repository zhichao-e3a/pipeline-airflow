from datetime import datetime

def extract_fetal_movement(raw_fmov, start_ts):

    fmov_idx = [] ; unique_time_set = set()
    start_dt = datetime.strptime(start_ts, '%Y-%m-%d %H:%M:%S')

    for _fmov in raw_fmov:

        fmov_unix   = int(_fmov.split('：')[1].split(' ')[0])
        fmov_deg    = _fmov.split('：')[2]
        fmov_dt     = datetime.fromtimestamp(fmov_unix)
        if fmov_dt < start_dt:
            continue
        idx         = fmov_dt-start_dt
        idx_s       = idx.seconds
        fmov_tuple  = (idx_s, fmov_deg)

        if idx_s not in unique_time_set:
            fmov_idx.append(fmov_tuple)
            unique_time_set.add(idx_s)

    fmov_idx.sort(key=lambda x: x[0])
    last = fmov_idx[-1][0]

    record = ["0" for _ in range(last)]

    for fm in fmov_idx:
        record[fm[0]-1] = fm[1]

    return record