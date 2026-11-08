import os
from pathlib import Path
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env.local", override=False, interpolate=False)
load_dotenv(PROJECT_ROOT / ".env", override=False, interpolate=False)


def required_env(name):
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"Missing {name}. Add it to your environment or .env file.")
    return value


def kafka_settings():
    return {
        "bootstrap.servers": required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": required_env("KAFKA_SASL_USERNAME"),
        "sasl.password": required_env("KAFKA_SASL_PASSWORD"),
        "client.id": "store",
    }


def database_settings():
    try:
        port = int(os.environ.get("DB_PORT", "5432"))
    except ValueError:
        raise ValueError("DB_PORT must be an integer.") from None

    return {
        "drivername": "postgresql+psycopg",
        "username": required_env("DB_USER"),
        "password": required_env("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": port,
    }
