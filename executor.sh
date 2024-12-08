#!/bin/bash

# Start the setup process
echo "Starting setup..."

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Inform the user that the environment is ready
echo "New environment created and activated!"

# Install dependencies
echo "Installing dependencies..."
python3 -m pip install -r requirements.txt 

# Run the application
echo "Running the application..."
python3 app.py
