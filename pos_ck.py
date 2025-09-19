import sys, csv
from pathlib import Path

BASE = Path(__file__).resolve().parent

def append_with_repo(src_csv: Path, dest_csv: Path, repo_full_name: str):
    if not src_csv.exists():
        return
    with open(src_csv, "r", encoding="utf-8-sig", newline="") as fsrc:
        reader = csv.DictReader(fsrc)
        src_header = reader.fieldnames or []
        out_header = src_header + (["repo_full_name"] if "repo_full_name" not in src_header else [])
        write_header = not dest_csv.exists()
        with open(dest_csv, "a", encoding="utf-8", newline="") as fdst:
            writer = csv.DictWriter(fdst, fieldnames=out_header)
            if write_header:
                writer.writeheader()
            for row in reader:
                row["repo_full_name"] = repo_full_name
                writer.writerow({k: row.get(k, "") for k in out_header})

def is_number(x):
    try:
        float(x)
        return True
    except:
        return False

def pick_col(header, candidates):
    for c in candidates:
        if c in header:
            return c
    return None

def aggregate_class_metrics(class_csv: Path):
    agg = {"num_classes": 0,"sum_class_loc": 0.0,"avg_class_wmc": 0.0,"avg_class_cbo": 0.0,"avg_class_rfc": 0.0,"avg_class_lcom": 0.0,"max_class_dit": 0.0}
    if not class_csv.exists():
        return agg
    colmap = {"wmc": ["wmc","WMC"],"cbo": ["cbo","CBO","cboModified","CBOModified"],"rfc": ["rfc","RFC"],"lcom": ["lcom","LCOM"],"dit": ["dit","DIT"],"loc": ["loc","LOC","locClass"]}
    totals = {"wmc": 0.0, "cbo": 0.0, "rfc": 0.0, "lcom": 0.0, "loc": 0.0, "n": 0}
    max_dit = None
    with open(class_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        c_wmc = pick_col(header, colmap["wmc"])
        c_cbo = pick_col(header, colmap["cbo"])
        c_rfc = pick_col(header, colmap["rfc"])
        c_lcom = pick_col(header, colmap["lcom"])
        c_dit = pick_col(header, colmap["dit"])
        c_loc = pick_col(header, colmap["loc"])
        for row in reader:
            totals["n"] += 1
            if c_wmc and is_number(row.get(c_wmc)): totals["wmc"] += float(row[c_wmc])
            if c_cbo and is_number(row.get(c_cbo)): totals["cbo"] += float(row[c_cbo])
            if c_rfc and is_number(row.get(c_rfc)): totals["rfc"] += float(row[c_rfc])
            if c_lcom and is_number(row.get(c_lcom)): totals["lcom"] += float(row[c_lcom])
            if c_loc and is_number(row.get(c_loc)): totals["loc"] += float(row[c_loc])
            if c_dit and is_number(row.get(c_dit)):
                v = float(row[c_dit])
                max_dit = v if (max_dit is None or v > max_dit) else max_dit
    n = totals["n"]
    agg["num_classes"] = n
    agg["sum_class_loc"] = totals["loc"]
    if n > 0:
        agg["avg_class_wmc"] = totals["wmc"]/n if totals["wmc"] else 0.0
        agg["avg_class_cbo"] = totals["cbo"]/n if totals["cbo"] else 0.0
        agg["avg_class_rfc"] = totals["rfc"]/n if totals["rfc"] else 0.0
        agg["avg_class_lcom"] = totals["lcom"]/n if totals["lcom"] else 0.0
    agg["max_class_dit"] = max_dit if max_dit is not None else 0.0
    return agg

def main():
    if len(sys.argv) < 2:
        print("Uso: python pos_ck.py <owner/repo>")
        sys.exit(1)
    repo_full_name = sys.argv[1].strip()
    owner, repo = repo_full_name.split("/", 1)
    out_dir = BASE / "ck_out" / owner / repo
    class_csv = out_dir / "class.csv"
    method_csv = out_dir / "method.csv"
    append_with_repo(class_csv, BASE / "ck_class_all.csv", repo_full_name)
    append_with_repo(method_csv, BASE / "ck_method_all.csv", repo_full_name)
    agg = aggregate_class_metrics(class_csv)
    with open(BASE / "repos_ck_agg.csv", "a", encoding="utf-8", newline="") as f:
        header = ["repo_full_name","num_classes","sum_class_loc","avg_class_wmc","avg_class_cbo","avg_class_rfc","avg_class_lcom","max_class_dit"]
        write_header = not (BASE / "repos_ck_agg.csv").exists()
        w = csv.DictWriter(f, fieldnames=header)
        if write_header:
            w.writeheader()
        w.writerow