import csv
import requests

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
]

def normalizar_repo(repo):
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
    }

def salvar_csv(repos):
    caminho_csv="repos.csv"
    with open(caminho_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CAMPOS)
        writer.writeheader()
        for repo in repos:
            writer.writerow(normalizar_repo(repo))
    print(f"CSV salvo em: {caminho_csv}")

with open('token.txt', 'r') as file:
    token = file.read().strip()

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "repo-fetcher/1.0"
}

base_url = "https://api.github.com/search/repositories"
params = {
    "q": "stars:>0",
    "sort": "stars",
    "order": "desc",
    "per_page": 100
}

todos_repos = []

for page in range(1, 11):
    params["page"] = page
    response = requests.get(base_url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        raise Exception(f"Erro na página {page}: {response.status_code} - {response.text}")

    data = response.json()
    items = data.get("items", [])

    if not items:
        print(f"Página {page}: 0 itens. Encerrando paginação.")
        break

    todos_repos.extend(items)
    print(f"Página {page}: {len(items)} itens (acumulado: {len(todos_repos)})")

todos_repos.sort(key=lambda r: r.get("stargazers_count") or 0, reverse=True)
salvar_csv(todos_repos)

