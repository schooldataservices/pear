# Use a base image with Python
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the script and modules
COPY main.py .
COPY modules ./modules


# Default command to run the script
CMD ["python", "main.py"]