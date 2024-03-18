### mle-project-sprint-1

### Яндекс Практикум, Курс "Инженер Машинного Обучения" 

### 1-й спринт, проект "Разработка пайплайнов подготовки данных и обучения модели"

#### Выполнил: Кирилл Н., ibnkir@yandex.ru

Этап 1. Сбор данных
- Функция DAG: prepare_flats_churn
- Файлы с Python-кодом:
	* dags/flats.py
	* plugins/steps/messages.py
	* notebooks/eda.ipynb

Этап 2. Очистка данных
- Функция DAG: prepare_clean_flats_churn
- Файлы с Python-кодом:
	* dags/clean_flats.py
	* plugins/steps/clean.py
	* plugins/steps/messages.py
	* notebooks/eda.ipynb

Этап 3. Создание DVC-пайплайна обучения модели
- Файлы с Python-кодом:
	* scripts/data.py
	* scripts/fit.py
	* scripts/evaluate.py
- Файлы с конфигурацией DVС-пайплайна:
	* dvc.yaml
	* params.yaml
	* dvc.lock
	