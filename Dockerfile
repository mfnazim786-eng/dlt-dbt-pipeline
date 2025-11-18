# 1. Use an official lightweight Python image
FROM python:3.11-slim

# 2. Set environment variables
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
# Cloud Run expects the app to listen on port 8080
ENV PORT 8080

# 3. Set the working directory
WORKDIR $APP_HOME

# 4. Copy and install dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy your application code into the container
COPY . .

# 6. Define the command to run your app
# The host 0.0.0.0 is essential to allow connections from outside the container.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]