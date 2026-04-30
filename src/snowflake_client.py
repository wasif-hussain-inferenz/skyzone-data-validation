import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.backends import default_backend


def get_connection(conn_params):
    """Create a Snowflake connection using private-key authentication."""

    key_file_path = conn_params.get("private_key_path", "rsa_key.p8")

    # If path is not absolute, make it relative to project root
    if not os.path.isabs(key_file_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        key_file_path = os.path.join(base_dir, key_file_path)

    # Read and decrypt the private key
    with open(key_file_path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=conn_params["private_key_passphrase"].encode("utf-8"),
            backend=default_backend()
        )

    private_key_bytes = p_key.private_bytes(
        encoding=Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=conn_params["user"],
        account=conn_params["account"],
        warehouse=conn_params["warehouse"],
        database=conn_params["database"],
        schema=conn_params["schema"],
        private_key=private_key_bytes,
    )
