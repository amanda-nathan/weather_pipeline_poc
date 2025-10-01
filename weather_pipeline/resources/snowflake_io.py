
from __future__ import annotations
import logging
import os
from pathlib import Path
import base64
import toml
from dagster import ConfigurableResource
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

class SnowflakeResource(ConfigurableResource):
    connection_name: str = "DEFAULT_CONNECTION"
    warehouse: str = "COMPUTE_WH"
    database: str = "WEATHER_DB"
    schema_name: str = "RAW"

    def _load_from_env(self):
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        pk_b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
        pk_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        pk_pass = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if not account or not user:
            return None
        private_key_bytes = None
        if pk_b64:
            pem = base64.b64decode(pk_b64.encode("utf-8"))
            p_key = serialization.load_pem_private_key(
                pem,
                password=(pk_pass.encode() if pk_pass else None),
                backend=default_backend(),
            )
            private_key_bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif pk_path:
            p = Path(pk_path)
            if not p.exists():
                raise FileNotFoundError(f"Private key file not found: {p}")
            with open(p, "rb") as f:
                p_key = serialization.load_pem_private_key(
                    f.read(),
                    password=(pk_pass.encode() if pk_pass else None),
                    backend=default_backend(),
                )
            private_key_bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            return None
        return account, user, private_key_bytes

    def _load_from_toml(self):
        toml_path = Path.home() / ".snowflake" / "connections.toml"
        if not toml_path.exists():
            return None
        config = toml.load(toml_path)
        conn_config = config.get(self.connection_name)
        if not conn_config:
            return None
        private_key_path = Path(conn_config["private_key_path"])
        with open(private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend(),
            )
        private_key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return conn_config["account"], conn_config["user"], private_key_bytes

    def get_engine(self):
        env_cfg = self._load_from_env()
        if env_cfg:
            account, user, private_key_bytes = env_cfg
        else:
            toml_cfg = self._load_from_toml()
            if not toml_cfg:
                raise RuntimeError("Missing Snowflake configuration")
            account, user, private_key_bytes = toml_cfg
        engine = create_engine(
            URL(
                account=account,
                user=user,
                database=self.database,
                schema=self.schema_name,
                warehouse=self.warehouse,
            ),
            connect_args={"private_key": private_key_bytes},
        )
        logger.info("Successfully created Snowflake SQLAlchemy engine.")
        return engine

snowflake_sqlalchemy_engine = SnowflakeResource
