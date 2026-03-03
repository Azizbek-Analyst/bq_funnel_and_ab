@echo off
REM Installing the bq library_funnel

REM Checking the presence of a virtual environment
if not exist venv (
    echo Creating a Python Virtual Environment...
    python -m venv venv
    echo The virtual environment has been created!
)

REM Activating the virtual environment
echo Activating the virtual environment...
call venv\Scripts\activate.bat

REM Update pip
echo pip update...
pip install --upgrade pip

REM Installing the library in developer mode
echo Installing the bq library_funnel...
pip install -e .

echo Installation complete! bq library_funnel is now available in your Python environment.
echo To start using the library, activate the virtual environment with the command:
echo venv\Scripts\activate.bat
