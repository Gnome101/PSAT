from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    # Database
    database_url: str = Field(validation_alias="DATABASE_URL")

    # GitHub
    github_token: str = Field(validation_alias="GITHUB_TOKEN")

    # DeFiLlama (no auth needed, but can set base URL)
    defillama_base_url: str = "https://api.llama.fi"

    # Etherscan
    etherscan_api_key: str = Field(validation_alias="ETHERSCAN_API_KEY")

    # X / Twitter — optional, defaults to empty so crawlers that don't need
    # it can still import config without the env var being set
    twitter_bearer_token: str = ""

    # Crawl settings
    default_rate_limit_rps: float = 1.0
    request_timeout_seconds: int = 30
    max_retries: int = 3
    max_concurrent_crawlers: int = 5

    # Scheduler intervals (in hours)
    docs_crawl_interval_hours: int = 24
    github_crawl_interval_hours: int = 6
    immunefi_crawl_interval_hours: int = 12
    governance_crawl_interval_hours: int = 1

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()  # type: ignore[call-arg]