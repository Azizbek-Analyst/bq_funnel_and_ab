#!/bin/bash
# Installing the bq library_funnel

# Checking the presence of a virtual environment
if [ ! -d "venv" ]; then
    echo "Creating a Python Virtual Environment..."
    python -m venv venv
    echo "The virtual environment has been created!"
fi

# Activating the virtual environment
echo "Activating the virtual environment..."
source venv/bin/activate

# Update pip
echo "pip update..."
pip install --upgrade pip

# Installing the library in developer mode
echo "Installing the bq library_funnel..."
pip install -e .

echo "Installation complete! bq library_funnel is now available in your Python environment."
echo "To start using the library, activate the virtual environment with the command:"
echo "source venv/bin/activate"
