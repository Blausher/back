from prometheus_client import Counter, Histogram

#  общее количество запросов
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
# время обработки запросов
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)

# Общее количество предсказаний модели, с лейблом result (violation / no_violation)
PREDICTIONS_TOTAL = Counter( 
    "predictions_total",
    "Total number of model predictions",
    ["result"],
)

#Время выполнения предсказания ML-моделью (только инференс)
PREDICTION_DURATION = Histogram(
    "prediction_duration_seconds",
    "Time spent on ML model inference",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

#Количество ошибок при предсказании, с лейблом error_type (model_unavailable / prediction_error)
PREDICTION_ERRORS_TOTAL = Counter(
    "prediction_errors_total",
    "Total number of model prediction errors",
    ["error_type"],
)

# Время выполнения запросов к PostgreSQL, с лейблом query_type (select / insert / update / delete)
DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds",
    "Time spent on PostgreSQL queries",
    ["query_type"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

#Распределение вероятностей нарушений от ML-модели
MODEL_PREDICTION_PROBABILITY = Histogram(
    "model_prediction_probability",
    "Distribution of violation probabilities returned by model",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
