FROM python:3.12-slim

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY leesah-game-client leesah-game-client

RUN ls

CMD ["python", "leesah-game-client"]
