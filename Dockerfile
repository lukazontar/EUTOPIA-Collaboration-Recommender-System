# Use the official Python 3.10 image from the Docker Hub
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container
COPY prod_requirements.txt .

# Install virtualenv
RUN pip install virtualenv

# Create a virtual environment
RUN python -m virtualenv venv

# Activate the virtual environment and install dependencies
RUN /bin/bash -c "source venv/bin/activate && pip install -r prod_requirements.txt"

# Copy the rest of your application code
 COPY . .

# Set the entrypoint to use the virtual environment by default
ENTRYPOINT ["/bin/bash", "-c", "source venv/bin/activate && exec \"$@\"", "--"]

# Default command to keep the container running
CMD ["tail", "-f", "/dev/null"]