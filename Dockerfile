# Use the base image provided by the infrastructure
FROM colav/impactu_airflow:base-latest

# Install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    python3-pybind11 \
    pybind11-dev \
    libhunspell-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project structure
COPY --chown=airflow:root . /opt/airflow/

# Install the project in the image to make all modules (extract, transform, etc.)
# available globally in the Python environment.
RUN pip install --no-cache-dir .

# Set the working directory
WORKDIR /opt/airflow
