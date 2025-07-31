# Use official Python runtime as a parent image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app



# Copy the rest of your project files into the container
COPY app/ .

# Expose the port your app listens on (assuming 6379 for Redis)
EXPOSE 6379

# Command to run your main script (adjust if needed)
CMD ["python", "main.py"]
