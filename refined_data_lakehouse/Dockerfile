FROM apache/spark:3.4.1-python3.9

# Install additional Python packages
RUN pip install --no-cache-dir \
    pyiceberg==0.5.0 \
    boto3==1.28.0 \
    botocore==1.31.0

# Set working directory
WORKDIR /app

# Copy the PySpark job script
COPY job_separated_approach.py .

# Copy configuration
COPY config.py .

# Create entrypoint script
RUN echo '#!/bin/bash\nspark-submit job_separated_approach.py "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"] 