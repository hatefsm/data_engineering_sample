from dataclasses import dataclass
import os

@dataclass(frozen=True)
class SourceDBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str = "prefer"

    def sqlalchemy_url(self) -> str:
        # psycopg3 driver
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}?sslmode={self.sslmode}"
        )

@dataclass(frozen=True)
class OutputConfig:
    writer_type: str         # local | (later: s3 | azure)
    bronze_base_path: str

@dataclass(frozen=True)
class AppConfig:
    source: SourceDBConfig
    output: OutputConfig

def load_config() -> AppConfig:
    return AppConfig(
        source=SourceDBConfig(
            host=os.getenv("SOURCE_DB_HOST", "localhost"),
            port=int(os.getenv("SOURCE_DB_PORT", "5432")),
            dbname=os.getenv("SOURCE_DB_NAME", "sample_source"),
            user=os.getenv("SOURCE_DB_USER", "demo_user"),
            password=os.getenv("SOURCE_DB_PASSWORD", "demo_pass"),
            sslmode=os.getenv("SOURCE_DB_SSLMODE", "prefer"),
        ),
        output=OutputConfig(
            writer_type=os.getenv("WRITER_TYPE", "local"),
            bronze_base_path=os.getenv("BRONZE_BASE_PATH", "./data/bronze"),
        ),
    )
