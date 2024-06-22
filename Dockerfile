FROM python:3.12-slim

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y openssl

RUN pip install --no-cache-dir -r requirements.txt

ARG SECRET_PASSWORD
ENV SECRET_PASSWORD=$SECRET_PASSWORD
RUN openssl enc -aes-256-cbc -d -in session.enc -out bot.session -pass pass:$SECRET_PASSWORD

CMD ["python", "main.py"]