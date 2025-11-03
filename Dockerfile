# Use AWS Lambda Python 3.9 base image for x86_64 (not arm64)
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.9

# Set working directory to /var/task (AWS Lambda default)
WORKDIR /var/task

# Copy application files into the container
COPY lambda_function.py .
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Set the entry point and handler correctly
ENTRYPOINT ["/var/lang/bin/python3.9", "-m", "awslambdaric"]
CMD ["lambda_function.lambda_handler"]
