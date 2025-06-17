FROM python:3.12-slim

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y openssl

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install torch==2.3.0+cpu --index-url https://download.pytorch.org/whl/cpu

ARG SECRET_PASSWORD
ENV SECRET_PASSWORD=$SECRET_PASSWORD
RUN python3 decrypt.py

CMD ["python", "main.py"]