"""
GitHubCrawler
=============
Uses the GitHub REST API (not HTML scraping) to pull security-relevant
content from a protocol's GitHub organisation.

Fetches per repo:
  - README.md
  - SECURITY.md
  - CHANGELOG.md  /  CHANGELOG
  - docs/ directory contents (recursively, up to depth 2)
  - Latest N releases (release notes as documents)
  - Open/closed issues labelled "security" or "vulnerability"
"""
import base64
import logging
from urllib.parse import urlparse

from base import BaseCrawler, CrawledDocument
from config import settings

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
TARGET_FILES = {"README.md", "SECURITY.md", "CHANGELOG.md", "CHANGELOG"}
MAX_RELEASES = 10
MAX_REPOS = 30
DOCS_DIRS = {"docs", "documentation", "wiki"}
SECURITY_LABELS = {"security", "vulnerability", "bug-bounty", "audit"}


class GitHubCrawler(BaseCrawler):
    CRAWLER_TYPE = "github"

    def __init__(self, protocol_id: str):
        super().__init__(protocol_id, rate_limit_rps=0.8)
        self._headers = {
            "Authorization": f"Bearer {settings.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def crawl(self, org_url: str) -> list[CrawledDocument]:
        if not org_url:
            return []

        org_name = self._extract_org_name(org_url)
        logger.info("[github] Crawling org: %s", org_name)

        repos = await self._list_repos(org_name)
        logger.info("[github] Found %d repos", len(repos))

        documents: list[CrawledDocument] = []
        for repo in repos[:MAX_REPOS]:
            repo_name = repo["name"]
            repo_docs = await self._crawl_repo(org_name, repo_name)
            documents.extend(repo_docs)
            await self.throttle()

        logger.info("[github] Done — %d documents", len(documents))
        return documents

    # ------------------------------------------------------------------
    # Repo-level crawl
    # ------------------------------------------------------------------

    async def _crawl_repo(self, org: str, repo: str) -> list[CrawledDocument]:
        docs: list[CrawledDocument] = []

        # Target files at repo root
        for filename in TARGET_FILES:
            doc = await self._fetch_file(org, repo, filename)
            if doc:
                docs.append(doc)

        # docs/ directories
        for dir_name in DOCS_DIRS:
            dir_docs = await self._crawl_directory(org, repo, dir_name, depth=0)
            docs.extend(dir_docs)

        # Releases (changelog signals)
        release_docs = await self._fetch_releases(org, repo)
        docs.extend(release_docs)

        # Security-labelled issues
        issue_docs = await self._fetch_security_issues(org, repo)
        docs.extend(issue_docs)

        return docs

    # ------------------------------------------------------------------
    # File fetching
    # ------------------------------------------------------------------

    async def _fetch_file(
        self, org: str, repo: str, path: str
    ) -> CrawledDocument | None:
        url = f"{GITHUB_API}/repos/{org}/{repo}/contents/{path}"
        try:
            resp = await self.safe_get(url, headers=self._headers)
            data = resp.json()

            if data.get("encoding") == "base64":
                content = base64.b64decode(data["content"]).decode("utf-8", errors="replace")
            else:
                content = data.get("content", "")

            if not content.strip():
                return None

            html_url = data.get("html_url", url)
            doc_type = self._classify_file(path)

            return self.make_document(
                source_url=html_url,
                raw_text=content,
                doc_type=doc_type,
                title=f"{org}/{repo}: {path}",
                metadata={
                    "org": org,
                    "repo": repo,
                    "path": path,
                    "sha": data.get("sha"),
                },
            )
        except Exception as exc:
            logger.debug("File not found %s/%s/%s: %s", org, repo, path, exc)
            return None

    # ------------------------------------------------------------------
    # Directory crawl
    # ------------------------------------------------------------------

    async def _crawl_directory(
        self, org: str, repo: str, dir_path: str, depth: int
    ) -> list[CrawledDocument]:
        if depth > 2:
            return []

        url = f"{GITHUB_API}/repos/{org}/{repo}/contents/{dir_path}"
        try:
            resp = await self.safe_get(url, headers=self._headers)
            items = resp.json()
        except Exception:
            return []

        docs: list[CrawledDocument] = []
        for item in items:
            item_type = item.get("type")
            item_path = item.get("path", "")

            if item_type == "file" and item_path.endswith((".md", ".txt", ".rst")):
                doc = await self._fetch_file(org, repo, item_path)
                if doc:
                    docs.append(doc)
                await self.throttle()

            elif item_type == "dir":
                sub_docs = await self._crawl_directory(org, repo, item_path, depth + 1)
                docs.extend(sub_docs)

        return docs

    # ------------------------------------------------------------------
    # Releases
    # ------------------------------------------------------------------

    async def _fetch_releases(
        self, org: str, repo: str
    ) -> list[CrawledDocument]:
        url = f"{GITHUB_API}/repos/{org}/{repo}/releases"
        try:
            resp = await self.safe_get(
                url,
                headers=self._headers,
                params={"per_page": MAX_RELEASES},
            )
            releases = resp.json()
        except Exception:
            return []

        docs: list[CrawledDocument] = []
        for release in releases:
            body = release.get("body", "") or ""
            name = release.get("name") or release.get("tag_name", "")
            if not body.strip():
                continue

            docs.append(
                self.make_document(
                    source_url=release.get("html_url", ""),
                    raw_text=body,
                    doc_type="github_release",
                    title=name,
                    metadata={
                        "org": org,
                        "repo": repo,
                        "tag": release.get("tag_name"),
                        "published_at": release.get("published_at"),
                        "prerelease": release.get("prerelease", False),
                    },
                )
            )
        return docs

    # ------------------------------------------------------------------
    # Security issues
    # ------------------------------------------------------------------

    async def _fetch_security_issues(
        self, org: str, repo: str
    ) -> list[CrawledDocument]:
        docs: list[CrawledDocument] = []
        for label in SECURITY_LABELS:
            url = f"{GITHUB_API}/repos/{org}/{repo}/issues"
            try:
                resp = await self.safe_get(
                    url,
                    headers=self._headers,
                    params={
                        "labels": label,
                        "state": "all",
                        "per_page": 20,
                    },
                )
                issues = resp.json()
            except Exception:
                continue

            for issue in issues:
                body = (issue.get("body") or "").strip()
                if not body:
                    continue
                docs.append(
                    self.make_document(
                        source_url=issue.get("html_url", ""),
                        raw_text=f"# {issue.get('title', '')}\n\n{body}",
                        doc_type="github_security_issue",
                        title=issue.get("title"),
                        metadata={
                            "org": org,
                            "repo": repo,
                            "label": label,
                            "state": issue.get("state"),
                            "created_at": issue.get("created_at"),
                        },
                    )
                )
            await self.throttle()
        return docs

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _list_repos(self, org: str) -> list[dict]:
        try:
            resp = await self.safe_get(
                f"{GITHUB_API}/orgs/{org}/repos",
                headers=self._headers,
                params={"per_page": MAX_REPOS, "sort": "updated"},
            )
            return resp.json()
        except Exception as exc:
            logger.error("[github] Failed to list repos for %s: %s", org, exc)
            return []

    def _extract_org_name(self, org_url: str) -> str:
        parsed = urlparse(org_url)
        return parsed.path.strip("/").split("/")[0]

    def _classify_file(self, path: str) -> str:
        name = path.split("/")[-1].lower()
        if name.startswith("security"):
            return "github_security_md"
        if name.startswith("changelog"):
            return "github_changelog"
        if name in {"readme.md", "readme"}:
            return "github_readme"
        return "github_doc"
