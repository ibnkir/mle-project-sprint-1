# mle-project-sprint-1

# Яндекс Практикум, Курс "Инженер Машинного Обучения" 

## 1-й спринт, проект "Разработка пайплайнов подготовки данных и обучения модели"

### Выполнил: Кирилл Н., ibnkir@yandex.ru

Этап 1. Сбор данных
- Функция DAG: prepare_churn_dataset
- Файлы с Python-кодом:
	a. dags/churn.py
	b. plugins/steps/messages.py

Этап 2. Очистка данных
- Функция DAG: clean_churn_dataset
- Файлы с Python-кодом:
	a. dags/clean_churn.py
	b. plugins/steps/clean.py
	c. plugins/steps/messages.py

Этап 3. Создание DVC-пайплайна обучения модели
- Файлы с Python-кодом:
	a. scripts/data.py
	b. scripts/fit.py
	c. scripts/evaluate.py
- Файлы с конфигурацией DVС-пайплайна:
	a. dvc.yaml
	b. params.yaml
	c. dvc.lock
	