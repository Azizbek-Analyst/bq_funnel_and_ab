"""
Модуль для настройки авторизации в Google Cloud и создания клиента BigQuery.
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings


def setup_bigquery_client(credentials_path=None, use_pydata_auth=False, scopes=None, project_id=None):
    """
    Настраивает клиент BigQuery с авторизацией.
    
    Args:
        credentials_path: Путь к файлу учетных данных сервисного аккаунта (JSON).
                         Если None, попытается использовать переменную окружения GOOGLE_APPLICATION_CREDENTIALS.
        use_pydata_auth: Использовать интерактивную аутентификацию через pydata_google_auth.
        scopes: Список необходимых разрешений для pydata_google_auth.
        project_id: ID проекта Google Cloud (при использовании pydata_google_auth).
    
    Returns:
        Авторизованный клиент BigQuery
    """
    # Вариант 1: Используем pydata_google_auth для интерактивной аутентификации
    if use_pydata_auth:
        try:
            import pydata_google_auth
            
            # Если списка разрешений нет, используем стандартные для BigQuery
            if scopes is None:
                scopes = [
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/drive'
                ]
            
            print(f"Запуск интерактивной аутентификации через pydata_google_auth...")
            
            # Получаем учетные данные через pydata_google_auth
            credentials = pydata_google_auth.get_user_credentials(
                scopes,
                auth_local_webserver=True,  # Использовать локальный веб-сервер для аутентификации
                client_id=None,             # Использовать клиент по умолчанию
                client_secret=None,         # Использовать клиент по умолчанию
            )
            
            # Создаем клиент с полученными учетными данными
            client = bigquery.Client(credentials=credentials, project=project_id)
            print(f"Аутентификация успешно завершена через pydata_google_auth")
            
            # Сохраняем информацию о текущем проекте
            if project_id:
                print(f"Используется проект: {project_id}")
            else:
                project_id = client.project
                print(f"Используется проект по умолчанию: {project_id}")
                
            return client
            
        except ImportError:
            warnings.warn("Библиотека pydata_google_auth не установлена. "
                          "Выполните 'pip install pydata-google-auth' для интерактивной аутентификации.")
            print("Продолжение с использованием стандартных методов аутентификации...")
        except Exception as e:
            warnings.warn(f"Ошибка при аутентификации через pydata_google_auth: {e}")
            print("Продолжение с использованием стандартных методов аутентификации...")
    
    # Вариант 2: Используем указанный файл учетных данных
    if credentials_path:
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=project_id)
            print(f"Авторизация с использованием учетных данных из файла: {credentials_path}")
            return client
        except Exception as e:
            warnings.warn(f"Ошибка при загрузке учетных данных из файла: {e}")
            print("Продолжение с использованием других методов аутентификации...")
    
    # Вариант 3: Используем переменную окружения GOOGLE_APPLICATION_CREDENTIALS
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        try:
            client = bigquery.Client(project=project_id)
            print(f"Авторизация с использованием GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
            return client
        except Exception as e:
            warnings.warn(f"Ошибка при использовании GOOGLE_APPLICATION_CREDENTIALS: {e}")
            print("Продолжение с использованием других методов аутентификации...")
    
    # Вариант 4: Используем метаданные GCP, если код запущен внутри GCP
    try:
        client = bigquery.Client(project=project_id)
        print("Авторизация с использованием метаданных GCP (запуск внутри GCP)")
        return client
    except Exception as e:
        # Если все методы аутентификации не сработали, выбрасываем исключение
        raise ValueError(
            "Не удалось авторизоваться в Google Cloud. Пожалуйста, выполните одно из следующих действий:\n"
            "1. Установите pydata-google-auth и используйте интерактивную аутентификацию: pip install pydata-google-auth\n"
            "2. Предоставьте путь к файлу учетных данных сервисного аккаунта\n"
            "3. Установите переменную окружения GOOGLE_APPLICATION_CREDENTIALS\n"
            "4. Убедитесь, что код запущен внутри GCP с соответствующими разрешениями\n"
            f"Подробности ошибки: {e}"
        ) from e


def authenticate_via_pydata(scopes=None, project_id=None):
    """
    Выполняет интерактивную аутентификацию через pydata_google_auth.
    
    Args:
        scopes: Список необходимых разрешений.
        project_id: ID проекта Google Cloud.
    
    Returns:
        Авторизованный клиент BigQuery
    """
    return setup_bigquery_client(use_pydata_auth=True, scopes=scopes, project_id=project_id)


def authenticate_with_service_account(credentials_path, project_id=None):
    """
    Выполняет аутентификацию с использованием файла ключа сервисного аккаунта.
    
    Args:
        credentials_path: Путь к файлу учетных данных сервисного аккаунта.
        project_id: ID проекта Google Cloud (необязательно).
        
    Returns:
        Авторизованный клиент BigQuery
    """
    return setup_bigquery_client(credentials_path=credentials_path, project_id=project_id)

def check_connection(client: bigquery.Client) -> bool:
    """
    Проверяет соединение с BigQuery, выполняя простой запрос.
    
    Args:
        client: Клиент BigQuery
        
    Returns:
        True, если соединение установлено успешно, иначе False
    """
    try:
        # Выполнение простого запроса для проверки соединения
        query = "SELECT 1"
        query_job = client.query(query)
        result = list(query_job.result())
        return len(result) > 0
    except Exception as e:
        print(f"Ошибка соединения с BigQuery: {str(e)}")
        return False