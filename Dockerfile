FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY output/model_config.json /output/model_config.json
ENTRYPOINT ["python", "src/main.py"]
