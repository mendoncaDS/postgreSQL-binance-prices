# Use Miniconda3 as base image
FROM python:3.10

# Set working directory
WORKDIR /usr/src/app

# Copy requirements.txt file to Docker image
COPY requirements.txt .

# Activate the environment and install dependencies using pip
RUN /bin/bash -c "pip install -r requirements.txt"

COPY update_database.py .
COPY run_bots.py .
COPY ping_telegram.py .
COPY begin.py .

EXPOSE 8080

ENTRYPOINT ["python", "begin.py"]