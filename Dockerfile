FROM python:3.11-slim-buster AS builder

RUN apt update && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt -t /python-deps

FROM python:3.11-slim-buster
COPY --from=builder /python-deps /usr/local/lib/python3.11/site-packages

WORKDIR /globant_challenge

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0","--port=8080"]

