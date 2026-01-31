# back

Запуск проекта
```bash
python main.py
```

Запуск тестов
```bash
python -m pytest -v
```

Последовательность инициализации БД
```bash
sudo -u postgres psql
CREATE DATABASE back OWNER blausher;
pgmigrate -t latest migrate
```