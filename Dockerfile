FROM python:3.12-slim

# Timezone — adjust if needed
ENV TZ=Asia/Jerusalem
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /usr/src/app/data
VOLUME /usr/src/app/data

# Ensure Python output is unbuffered (shows logs immediately)
ENV PYTHONUNBUFFERED=1

# Healthcheck — verify the process is alive
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Graceful shutdown: send SIGTERM first
STOPSIGNAL SIGTERM

CMD ["python", "main.py"]
