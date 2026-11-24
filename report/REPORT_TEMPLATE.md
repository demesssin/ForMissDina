# Шаблон отчёта (преобразуйте в PDF: Word / Google Docs → Экспорт в PDF)

Правило: каждое число и скриншот берите из **своего реального запуска** (`docs/pipeline_summary.md`, вывод консоли, `figures/`). Не копируйте цифры из README.

| № | Раздел | Что вставить и откуда взять |
|---|---|---|
| 1 | Титульный лист | название, курс, участники, номер группы, дата |
| 2 | Введение | зачем анализировать волатильность крипты; цели; 4 исследовательских вопроса (см. `src/analysis.py`) |
| 3 | Источник данных | эндпоинт, параметры (`symbol`, `interval`, `limit`), `src/api_client.py`, `head()` из `data/raw_data.csv`, таблица полей из `docs/data_dictionary.md` |
| 4 | Архитектура | `figures/architecture.png`, описание каждого модуля, поток данных (CSV → Kafka → CSV → ...) |
| 5 | Kafka | `docker-compose.yml`, топик (3 партиции), фрагменты `producer.py`/`consumer.py`, JSON `dumps/loads`, скриншот `Published: N` и `Consumed: N`, условие остановки consumer, обработка ошибок |
| 6 | Pandera | правила (10 штук, список в докстринге `validator.py`), вывод `python -m src.validator`, `data/invalid_records.csv`, результат `python -m tests.test_validator`, стратегия карантина |
| 7 | Очистка | вывод `python -m src.cleaner`: пропуски, дубликаты, типы, NumPy, фильтр, GroupBy, pivot_table/melt; блоки «WHY» |
| 8 | SQLite | `sql/schema.sql`, PK, вывод `python -m src.loader` (два запуска), результаты SQL-запросов |
| 9 | EDA | вывод `python -m src.analysis`, 6 графиков из `figures/`, интерпретации из `figures/eda_summary.md` (перефразируйте своими словами) |
| 10 | Тестирование | таблица счётчиков из `docs/pipeline_summary.md`, объяснение разницы (5010 − 5000 = 5 тестовых + 5 дубликатов − 5 удалено ...), идемпотентность, встреченные ошибки |
| 11 | Выводы | функциональность, главные находки, трудности, ограничения (один источник, ~3 недели часовых данных, нет real-time) |
| 12 | Вклад участников | кто что делал (см. таблицу распределения) |
| 13 | Литература | документация Binance Spot API, Apache Kafka, confluent-kafka-python, Pandera, pandas, NumPy, SQLite, Matplotlib/Seaborn |
