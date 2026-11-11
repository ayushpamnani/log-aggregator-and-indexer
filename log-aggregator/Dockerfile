FROM python:3.12-slim
WORKDIR /app
COPY logagg ./logagg
EXPOSE 8080
VOLUME /app/data
CMD ["python", "-m", "logagg", "--host", "0.0.0.0", "--port", "8080", "--data-dir", "/app/data"]
