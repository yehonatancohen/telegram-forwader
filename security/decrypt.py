from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
import os
from pathlib import Path
dotenv_path = Path('./config.env')

def decrypt_file(file_path, password):
    with open(file_path, 'rb') as f:
        data = f.read()
    
    salt = data[:16]
    iv = data[16:32]
    encrypted_data = data[32:]
    
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = kdf.derive(password)
    
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(encrypted_data) + decryptor.finalize()
    
    unpadder = padding.PKCS7(128).unpadder()
    data = unpadder.update(padded_data) + unpadder.finalize()
    
    with open(file_path.replace('.enc', ''), 'wb') as f:
        f.write(data)

load_dotenv(dotenv_path=dotenv_path)
# Use the secret password from environment variable
secret_password = os.getenv('SECRET_PASSWORD').encode()

decrypt_file('bot.session.enc', secret_password)
