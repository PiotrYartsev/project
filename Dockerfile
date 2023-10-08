# Use an official Python runtime as a parent image
FROM python:3.10.12

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies your Python code requires
RUN pip install -r requirements.txt  # If you have a requirements.txt file

# Make your Bash script executable
RUN chmod +x your_script.sh

# Define the command to run your application
CMD ["python", "your_python_script.py"]
