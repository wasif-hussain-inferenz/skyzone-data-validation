import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.backends import default_backend


def get_connection(conn_params):
    """Create Snowflake connection - supports both key-based and externalbrowser auth"""
    
    # Check if using key-based authentication
    if "private_key_path" in conn_params and conn_params.get("private_key_path"):
        # Key-based authentication
        key_file_path = conn_params.get("private_key_path", "rsa_key.p8")
        
        # If path is not absolute, make it relative to project root
        if not os.path.isabs(key_file_path):
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            key_file_path = os.path.join(base_dir, key_file_path)
        
        # Read and decrypt the private key
        with open(key_file_path, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=conn_params["private_key_passphrase"].encode('utf-8'),
                backend=default_backend()
            )
        
        # Convert to bytes
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
    
    # Otherwise use externalbrowser authentication
    else:
        chrome_path = os.environ.get(
            "SNOWFLAKE_BROWSER",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        )

        if not Path(chrome_path).exists():
            chrome_path = "chrome"

        os.environ["BROWSER"] = chrome_path

        return snowflake.connector.connect(
            user=conn_params["user"],
            account=conn_params["account"],
            warehouse=conn_params["warehouse"],
            database=conn_params["database"],
            schema=conn_params["schema"],
            authenticator=conn_params.get("authenticator", "externalbrowser")
        )
