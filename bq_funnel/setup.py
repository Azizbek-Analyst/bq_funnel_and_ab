from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="bq_funnel",
    version="0.1.0",
    author="Ваше Имя",
    author_email="ваш.email@example.com",
    description="Библиотека для анализа воронок пользователей в BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/bq_funnel",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "google-cloud-bigquery>=2.0.0",
        "google-cloud-bigquery-storage>=2.0.0",  # Добавлен для более быстрой загрузки данных
        "pandas>=1.0.0",
        "numpy>=1.18.0",
        "matplotlib>=3.2.0",
        "seaborn>=0.10.0",
        "scipy>=1.4.0",
        "pydata-google-auth>=1.0.0",  # Добавлен для интерактивной аутентификации
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
        ],
    },
)