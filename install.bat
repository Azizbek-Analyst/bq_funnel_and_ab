@echo off
REM Установка библиотеки bq_funnel

REM Проверяем наличие виртуальной среды
if not exist venv (
    echo Создание виртуальной среды Python...
    python -m venv venv
    echo Виртуальная среда создана!
)

REM Активируем виртуальную среду
echo Активация виртуальной среды...
call venv\Scripts\activate.bat

REM Обновляем pip
echo Обновление pip...
pip install --upgrade pip

REM Устанавливаем библиотеку в режиме разработчика
echo Установка библиотеки bq_funnel...
pip install -e .

echo Установка завершена! Библиотека bq_funnel теперь доступна в вашей Python-среде.
echo Чтобы начать использовать библиотеку, активируйте виртуальную среду командой:
echo venv\Scripts\activate.bat
