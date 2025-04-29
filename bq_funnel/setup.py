from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="bq_funnel",
    version="0.1.0",
    author="Azizbek Abdrakhimov",
    author_email="azizbek.abdrakhimov@gmail.com",
    description="Библиотека для анализа воронок пользователей в BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/bq_funnel",
    packages=find_packages(),
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
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
         "pyarrow>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
             "isort>=5.0.0",
        ],
    },
)