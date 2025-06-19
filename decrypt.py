import os
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.backends import default_backend

PASSWORD = os.getenv("SECRET_PASSWORD")
if not PASSWORD:
    raise SystemExit("SECRET_PASSWORD env-var not set")
PASSWORD = PASSWORD.encode()          # bytes

SESS_DIR = Path(os.getenv("SESSION_DIR", "."))   # mount /app/sessions in Koyeb

def _decrypt_file(enc_path: Path, key_pwd: bytes) -> None:
    clear_path = enc_path.with_suffix("")  # strip ".enc"
    if clear_path.exists():
        print(f"✓ {clear_path.name} already present – skipped")
        return

    data = enc_path.read_bytes()
    salt, iv, encrypted = data[:16], data[16:32], data[32:]

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100_000,
        backend=default_backend(),
    )
    key = kdf.derive(key_pwd)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    padded = cipher.decryptor().update(encrypted) + cipher.decryptor().finalize()

    unpadder = padding.PKCS7(128).unpadder()
    clear = unpadder.update(padded) + unpadder.finalize()

    clear_path.write_bytes(clear)
    print(f"✓ Decrypted {enc_path.name} → {clear_path.name}")

def decrypt_all(directory: Path, key_pwd: bytes) -> None:
    for enc in directory.glob("*.session.enc"):
        try:
            _decrypt_file(enc, key_pwd)
        except Exception as exc:
            print(f"✗ Failed decrypting {enc.name}: {exc}")

if __name__ == "__main__":
    decrypt_all(SESS_DIR, PASSWORD)