import csv
import time
import requests
from datetime import datetime, timezone

CAMPOS = [
    "id",
    "name",
    "full_name",
    "html_url",
    "description",
    "language",
    "stargazers_count",
    "forks_count",
    "open_issues_count",
    "watchers_count",
    "created_at",
    "updated_at",
    "pushed_at",
    "size",
    "default_branch",
    "license_spdx_id",
    "owner_login",
    "owner_type",
    "private",
    "archived",
    "releases_count",
    "age_years",
]

def normalizar_repo(repo, releases_count, age_years):
    return {
        "id": repo.get("id"),
        "name": repo.get("name"),
        "full_name": repo.get("full_name"),
        "html_url": repo.get("html_url"),
        "description": (repo.get("description") or "").replace("\r", " ").replace("\n", " "),
        "language": repo.get("language"),
        "stargazers_count": repo.get("stargazers_count"),
        "forks_count": repo.get("forks_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "watchers_count": repo.get("watchers_count"),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
        "size": repo.get("size"),
        "default_branch": repo.get("default_branch"),
        "license_spdx_id": (repo.get("license") or {}).get("spdx_id"),
        "owner_login": (repo.get("owner") or {}).get("login"),
        "owner_type": (repo.get("owner") or {}).get("type"),
        "private": repo.get("private"),
        "archived": repo.get("archived"),
        "releases_count": releases_count,
        "age_years": age_years,
    }

def salvar_csv(repos_norm):
    caminho_csv = "repos.csv"
    with open(caminho_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CAMPOS)
        writer.writeheader()
        for row in repos_norm:
            writer.writerow(row)
    print(f"CSV salvo em: {caminho_csv}")

def calcular_idade_anos(created_at_iso):
    if not created_at_iso:
        return None
    dt = datetime.fromisoformat(created_at_iso.replace("Z", "+00:00"))
    delta = datetime.now(timezone.utc) - dt
    return round(delta.days / 365.25, 3)

def get_releases_count(session, headers, owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    resp = session.get(url, headers=headers, params={"per_page": 1}, timeout=30)
    if resp.status_code == 404:
        return 0
    if resp.status_code == 403:
        time.sleep(2)
        resp = session.get(url, headers=headers, params={"per_page": 1}, timeout=30)
    resp.raise_for_status()
    link = resp.headers.get("Link", "")
    if "rel=\"last\"" in link:
        try:
            last_part = [p for p in link.split(",") if 'rel="last"' in p][0]
            last_url = last_part.split(";")[0].strip().strip("<>")
            import urllib.parse as up
            parsed = up.urlparse(last_url)
            qs = up.parse_qs(parsed.query)
            return int(qs.get("page", [1])[0])
        except Exception:
            pass
    try:
        return len(resp.json())
    except Exception:
        return None

with open('token.txt', 'r') as file:
    token = file.read().strip()

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "repo-fetcher/1.0"
}

base_url = "https://api.github.com/search/repositories"
params = {
    "q": "language:Java stars:>0",
    "sort": "stars",
    "order": "desc",
    "per_page": 100
}

todos_repos_norm = []
session = requests.Session()

for page in range(1, 11):
    params["page"] = page
    response = session.get(base_url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        raise Exception(f"Erro na página {page}: {response.status_code} - {response.text}")

    data = response.json()
    items = data.get("items", [])

    if not items:
        print(f"Página {page}: 0 itens. Encerrando paginação.")
        break

    print(f"Página {page}: {len(items)} itens")

    for repo in items:
        owner = (repo.get("owner") or {}).get("login")
        name = repo.get("name")
        releases_count = get_releases_count(session, headers, owner, name)
        age_years = calcular_idade_anos(repo.get("created_at"))
        todos_repos_norm.append(normalizar_repo(repo, releases_count, age_years))
        time.sleep(0.15)

todos_repos_norm.sort(key=lambda r: r.get("stargazers_count") or 0, reverse=True)
salvar_csv(todos_repos_norm)