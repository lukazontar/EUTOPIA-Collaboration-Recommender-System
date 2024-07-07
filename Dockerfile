# Use a base image with Conda installed
FROM continuumio/miniconda3

# Set the working directory
WORKDIR /app

# Copy the environment.yml and the requirements.txt files into the Docker container
COPY environment.yml .

# Create the Conda environment
RUN conda env create -f environment.yml

# Activate the Conda environment
RUN conda activate eutopia-env-1

# Copy the rest of your application code into the Docker container
COPY . .