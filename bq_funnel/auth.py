"""
Module for setting up authorization in Google Cloud and creating a BigQuery client.
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings


def setup_bigquery_client(credentials_path=None, use_pydata_auth=False, scopes=None, project_id=None):
    """
    Sets up a BigQuery client with authorization.
    
    Args:
        credentials_path: Path to the service account credentials file (JSON).
                         If None, will try to use the GOOGLE environment variable_APPLICATION_CREDENTIALS.
        use_pydata_auth: Use interactive authentication via pydata_google_auth.
        scopes: List of required permissions for pydata_google_auth.
        project_id: ID Google Cloud project (using pydata_google_auth).
    
    Returns:
        Authorized BigQuery client
    """
    # Option 1: Use pydata_google_auth for interactive authentication
    if use_pydata_auth:
        try:
            import pydata_google_auth
            
            # If there is no list of permissions, we use the standard ones for BigQuery
            if scopes is None:
                scopes = [
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/drive'
                ]
            
            print(f"Running interactive authentication via pydata_google_auth...")
            
            # Getting credentials via pydata_google_auth
            credentials = pydata_google_auth.get_user_credentials(
                scopes,
                auth_local_webserver=True,  # Use local web server for authentication
                client_id=None,             # Use default client
                client_secret=None,         # Use default client
            )
            
            # Create a client with the received credentials
            client = bigquery.Client(credentials=credentials, project=project_id)
            print(f"Authentication completed successfully via pydata_google_auth")
            
            # We save information about the current project
            if project_id:
                print(f"Project used: {project_id}")
            else:
                project_id = client.project
                print(f"The default project is used: {project_id}")
                
            return client
            
        except ImportError:
            warnings.warn("pydata library_google_auth not installed. "
                          "Execute 'pip install pydata-google-auth' for interactive authentication.")
            print("Continue using standard authentication methods...")
        except Exception as e:
            warnings.warn(f"Error authenticating via pydata_google_auth: {e}")
            print("Continue using standard authentication methods...")
    
    # Option 2: Use the specified credentials file
    if credentials_path:
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=project_id)
            print(f"Authorization using credentials from file: {credentials_path}")
            return client
        except Exception as e:
            warnings.warn(f"Error loading credentials from file: {e}")
            print("Continue with other authentication methods...")
    
    # Option 3: Use the GOOGLE environment variable_APPLICATION_CREDENTIALS
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        try:
            client = bigquery.Client(project=project_id)
            print(f"Authorization using GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
            return client
        except Exception as e:
            warnings.warn(f"Error using GOOGLE_APPLICATION_CREDENTIALS: {e}")
            print("Continue with other authentication methods...")
    
    # Option 4: Use GCP metadata if the code is running inside GCP
    try:
        client = bigquery.Client(project=project_id)
        print("Authorization using GCP metadata (running inside GCP)")
        return client
    except Exception as e:
        # If all authentication methods fail, throw an exception
        raise ValueError(
            "Failed to login to Google Cloud. Please do one of the following:\n"
            "1. Install pydata-google-auth and use interactive authentication: pip install pydata-google-auth\n"
            "2. Provide the path to the service account credentials file\n"
            "3. Set the GOOGLE environment variable_APPLICATION_CREDENTIALS\n"
            "4. Make sure the code is running inside GCP with the appropriate permissions\n"
            f"Error details: {e}"
        ) from e


def authenticate_via_pydata(scopes=None, project_id=None):
    """
    Performs interactive authentication via pydata_google_auth.
    
    Args:
        scopes: List of required permissions.
        project_id: ID Google Cloud project.
    
    Returns:
        Authorized BigQuery client
    """
    return setup_bigquery_client(use_pydata_auth=True, scopes=scopes, project_id=project_id)


def authenticate_with_service_account(credentials_path, project_id=None):
    """
    Performs authentication using the service account key file.
    
    Args:
        credentials_path: Path to the service account credentials file.
        project_id: ID Google Cloud project (optional)).
        
    Returns:
        Authorized BigQuery client
    """
    return setup_bigquery_client(credentials_path=credentials_path, project_id=project_id)

def check_connection(client: bigquery.Client) -> bool:
    """
    Tests connection to BigQuery by running a simple query.
    
    Args:
        client: BigQuery client
        
    Returns:
        True, if the connection is successful, otherwise False
    """
    try:
        # Running a simple query to test the connection
        query = "SELECT 1"
        query_job = client.query(query)
        result = list(query_job.result())
        return len(result) > 0
    except Exception as e:
        print(f"Error connecting to BigQuery: {str(e)}")
        return False