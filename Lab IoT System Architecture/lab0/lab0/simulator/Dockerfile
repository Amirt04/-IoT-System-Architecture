FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulator.py .

# -u: вывод без буферизации, чтобы логи сразу шли в docker logs
CMD ["python", "-u", "simulator.py"]
