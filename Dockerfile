FROM python:3.12-slim

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y openssl

RUN pip install --no-cache-dir -r requirements.txt

ARG SECRET_PASSWORD
ENV SECRET_PASSWORD=$SECRET_PASSWORD
RUN python3 decrypt.py

CMD ["python", "main.py"]