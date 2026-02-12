# back

Запуск проекта
```bash
docker compose up -d
```

Запуск тестов
```bash
python -m pytest -v
```

Последовательность инициализации БД
```bash
sudo -u postgres psql
CREATE DATABASE back OWNER blausher;

sudo -u postgres psql -d back
GRANT ALL PRIVILEGES ON DATABASE back TO blausher;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE users TO blausher;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE advertisements TO blausher;

pgmigrate -t latest migrate
```
