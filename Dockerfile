FROM python:3.9-slim

WORKDIR /app

# Install required packages for psycopg2
RUN apt-get update && \
    apt-get install -y \
    libpq-dev \
    gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install psycopg2
RUN pip install psycopg2-binary

# Copy the elt directory
COPY elt/ elt/

CMD ["python", "elt/elt_script.py"]