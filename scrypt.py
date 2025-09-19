import os, csv, sys, shutil, zipfile, tempfile, subprocess
from pathlib import Path
import requests

BASE = Path(__file__).resolve().parent
CK_JAR = os.environ.get("CK_JAR", str(BASE / "ck.jar"))
OUTPUT_BASE = BASE / "ck_out"
WORK_BASE = BASE / "work_one"
LOG_BASE = BASE / "ck_logs"
REQUEST_TIMEOUT = 180
JAVA_TIMEOUT = 1200
USE_JARS = "true"
MAX_FILES_PER_PARTITION = "0"
VARIABLES_AND_FIELDS = "false"
DO_CLEANUP_SRC = False

def ensure_java_and_ck():
    if not Path(CK_JAR).exists():
        raise FileNotFoundError(f"CK jar não encontrado em '{CK_JAR}'")
    subprocess.run(["java", "-version"], capture_output=True, check=True)

def load_csv_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    if not rows:
        raise RuntimeError("repos.csv vazio")
    return rows

def extract_full_name(row):
    for k in ["repo_full_name", "full_name", "name_with_owner"]:
        if k in row and row[k]:
            return row[k].strip()
    if "owner" in row and "name" in row and row["owner"] and row["name"]:
        return f'{row["owner"].strip()}/{row["name"].strip()}'
    for k in ["html_url", "url", "repo_url"]:
        if k in row and row[k]:
            s = row[k].split("github.com/")[-1].split("/")
            if len(s) >= 2:
                return f"{s[0]}/{s[1]}".strip()
    return None

def select_from_csv(rows, selector):
    selector = selector.strip()
    if selector.isdigit():
        idx = int(selector)
        if idx <= 0 or idx > len(rows):
            raise IndexError("índice fora do intervalo")
        row = rows[idx-1]
        fn = extract_full_name(row)
        if not fn:
            raise ValueError("full_name ausente na linha selecionada")
        return fn
    s = selector
    if s.startswith("http://") or s.startswith("https://"):
        parts = s.split("github.com/")[-1].split("/")
        if len(parts) < 2:
            raise ValueError("URL inválida")
        s = f"{parts[0]}/{parts[1]}"
    for row in rows:
        fn = extract_full_name(row)
        if fn and fn.lower() == s.lower():
            return fn
    if "/" in s and len(s.split("/")) == 2:
        return s
    raise ValueError("repositório não encontrado em repos.csv")

def get_session_and_headers():
    s = requests.Session()
    h = {"Accept": "application/vnd.github+json", "User-Agent": "ck-one-from-csv/1.0"}
    try:
        with open(BASE / "token.txt", "r", encoding="utf-8") as f:
            token = f.read().strip()
        if token:
            h["Authorization"] = f"token {token}"
    except:
        pass
    return s, h

def get_default_branch(session, headers, owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}"
    r = session.get(url, headers=headers, timeout=60)
    if r.status_code == 404:
        raise FileNotFoundError("repositório não encontrado")
    r.raise_for_status()
    data = r.json()
    if data.get("archived"):
        raise RuntimeError("repositório arquivado")
    return data.get("default_branch") or "master"

def download_and_extract_zipball(session, headers, owner, repo, branch, dest_dir: Path):
    url = f"https://api.github.com/repos/{owner}/{repo}/zipball/{branch}"
    r = session.get(url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
    if r.status_code == 404:
        url = f"https://api.github.com/repos/{owner}/{repo}/zipball"
        r = session.get(url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    try:
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                tmp_zip.write(chunk)
        tmp_zip.close()
        tmp_extract_dir = Path(tempfile.mkdtemp(prefix="repo_extract_"))
        try:
            with zipfile.ZipFile(tmp_zip.name, "r") as zf:
                zf.extractall(tmp_extract_dir)
            roots = [p for p in tmp_extract_dir.iterdir() if p.is_dir()]
            if not roots:
                raise RuntimeError("zip sem diretório raiz")
            root = roots[0]
            if dest_dir.exists():
                shutil.rmtree(dest_dir, ignore_errors=True)
            dest_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(root), str(dest_dir))
        finally:
            shutil.rmtree(tmp_extract_dir, ignore_errors=True)
    finally:
        try:
            os.unlink(tmp_zip.name)
        except:
            pass

def count_java_files(dir_path: Path):
    c = 0
    for _ in dir_path.rglob("*.java"):
        c += 1
    return c

def guess_java_root(src_dir: Path):
    candidates = [
        "src/main/java",
        "app/src/main/java",
        "src",
        "source",
        "sources",
        "codes/java",
        "code/java",
        "java",
        "lib"
    ]
    for c in candidates:
        p = src_dir / c
        if p.exists() and p.is_dir():
            if count_java_files(p) > 0:
                return p
    best_dir = None
    best_count = 0
    for d in src_dir.rglob("*"):
        if d.is_dir():
            cnt = 0
            try:
                for _ in d.rglob("*.java"):
                    cnt += 1
            except:
                cnt = 0
            if cnt > best_count:
                best_count = cnt
                best_dir = d
    return best_dir if best_count > 0 else None

def run_ck_on_root(java_root: Path, output_dir: Path, log_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    args = ["java","-jar",CK_JAR,str(java_root),USE_JARS,MAX_FILES_PER_PARTITION,VARIABLES_AND_FIELDS,str(output_dir)]
    res = subprocess.run(args, capture_output=True, text=True, timeout=JAVA_TIMEOUT)
    (log_dir / "ck_stdout.txt").write_text(res.stdout or "", encoding="utf-8")
    (log_dir / "ck_stderr.txt").write_text(res.stderr or "", encoding="utf-8")
    if res.returncode != 0:
        raise RuntimeError(f"CK falhou. Veja {log_dir/'ck_stderr.txt'}")

def main():
    if len(sys.argv) == 1:
        print("Uso: python scrypt.py <seletor> [branch]\n       ou: python scrypt.py <caminho_repos.csv> <seletor> [branch]")
        sys.exit(1)
    if len(sys.argv) == 2:
        csv_path = BASE / "repos.csv"
        selector = sys.argv[1]
        branch = None
    else:
        csv_path = Path(sys.argv[1])
        if not csv_path.is_absolute():
            csv_path = BASE / csv_path
        selector = sys.argv[2]
        branch = sys.argv[3] if len(sys.argv) >= 4 else None
    ensure_java_and_ck()
    rows = load_csv_rows(csv_path)
    full_name = select_from_csv(rows, selector)
    owner, repo = full_name.split("/", 1)
    session, headers = get_session_and_headers()
    if not branch:
        branch = get_default_branch(session, headers, owner, repo)
    src_dir = WORK_BASE / owner / repo
    out_dir = OUTPUT_BASE / owner / repo
    log_dir = LOG_BASE / owner / repo
    shutil.rmtree(src_dir, ignore_errors=True)
    print(f"Baixando {owner}/{repo}@{branch}")
    download_and_extract_zipball(session, headers, owner, repo, branch, src_dir)
    total_java = count_java_files(src_dir)
    print(f".java encontrados (repo completo): {total_java}")
    java_root = guess_java_root(src_dir)
    if not java_root:
        print("Nenhum .java encontrado. Escolha outro repositório.")
        sys.exit(0)
    print(f"Executando CK em: {java_root}")
    run_ck_on_root(java_root, out_dir, log_dir)
    class_csv = out_dir / "class.csv"
    lines = 0
    if class_csv.exists():
        with open(class_csv, "r", encoding="utf-8-sig") as f:
            lines = sum(1 for _ in f)
    print(f"class.csv linhas: {lines}")
    print(f"OK: {out_dir}")
    if DO_CLEANUP_SRC:
        shutil.rmtree(src_dir, ignore_errors=True)

if __name__ == "__main__":
    main()