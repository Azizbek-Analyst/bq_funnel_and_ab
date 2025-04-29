#!/bin/bash
# Установка библиотеки bq_funnel

# Проверяем наличие виртуальной среды
if [ ! -d "venv" ]; then
    echo "Создание виртуальной среды Python..."
    python -m venv venv
    echo "Виртуальная среда создана!"
fi

# Активируем виртуальную среду
echo "Активация виртуальной среды..."
source venv/bin/activate

# Обновляем pip
echo "Обновление pip..."
pip install --upgrade pip

# Устанавливаем библиотеку в режиме разработчика
echo "Установка библиотеки bq_funnel..."
pip install -e .

echo "Установка завершена! Библиотека bq_funnel теперь доступна в вашей Python-среде."
echo "Чтобы начать использовать библиотеку, активируйте виртуальную среду командой:"
echo "source venv/bin/activate"
