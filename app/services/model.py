from __future__ import annotations

import importlib
import os
import pickle
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

DEFAULT_MODEL_PATH = "model.pkl"
DEFAULT_MLFLOW_EXPERIMENT_NAME = "moderation-model"
DEFAULT_MLFLOW_MODEL_NAME = "moderation-model"
DEFAULT_MLFLOW_MODEL_ALIAS = "champion"


@dataclass(frozen=True)
class ModelSettings:
    use_mlflow: bool
    tracking_uri: str | None
    experiment_name: str
    model_name: str
    model_alias: str


def train_model():
    """Обучает простую модель на синтетических данных."""
    from sklearn.linear_model import LogisticRegression

    np.random.seed(42)
    # Признаки: [is_verified_seller, images_qty, description_length, category]
    X = np.random.rand(1000, 4)
    # Целевая переменная: 1 = нарушение, 0 = нет нарушения
    y = (X[:, 0] < 0.3) & (X[:, 1] < 0.2)
    y = y.astype(int)

    model = LogisticRegression()
    model.fit(X, y)
    return model


def save_model(model, path: str | Path = DEFAULT_MODEL_PATH):
    with open(path, "wb") as f:
        pickle.dump(model, f)


def load_model(path: str | Path = DEFAULT_MODEL_PATH):
    with open(path, "rb") as f:
        return pickle.load(f)


def parse_bool_env(value: str | None, default: bool = False) -> bool:
    """Преобразует env-строку в bool."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_model_settings(use_mlflow: bool | None = None) -> ModelSettings:
    """Собирает настройки источника модели из env."""
    resolved_use_mlflow = parse_bool_env(os.getenv("USE_MLFLOW"), default=False)
    if use_mlflow is not None:
        resolved_use_mlflow = use_mlflow

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if tracking_uri is not None:
        tracking_uri = tracking_uri.strip() or None

    return ModelSettings(
        use_mlflow=resolved_use_mlflow,
        tracking_uri=tracking_uri,
        experiment_name=os.getenv(
            "MLFLOW_EXPERIMENT_NAME",
            DEFAULT_MLFLOW_EXPERIMENT_NAME,
        ),
        model_name=os.getenv("MLFLOW_MODEL_NAME", DEFAULT_MLFLOW_MODEL_NAME),
        model_alias=os.getenv("MLFLOW_MODEL_ALIAS", DEFAULT_MLFLOW_MODEL_ALIAS),
    )


def load_or_train_model(
    path: str | Path = DEFAULT_MODEL_PATH,
    use_mlflow: bool | None = None,
):
    """Загружает модель из локального файла или из MLflow Registry."""
    settings = get_model_settings(use_mlflow=use_mlflow)
    if settings.use_mlflow:
        return load_or_train_model_from_mlflow(settings)
    return load_or_train_local_model(path)


def load_or_train_local_model(path: str | Path = DEFAULT_MODEL_PATH):
    """Сохраняет текущее поведение: локальный pickle с lazy training."""
    model_path = Path(path)
    if model_path.exists():
        return load_model(model_path)

    model = train_model()
    save_model(model, model_path)
    return model


def load_or_train_model_from_mlflow(settings: ModelSettings):
    """Загружает модель из MLflow Registry, при необходимости регистрируя новую."""
    validate_mlflow_settings(settings)
    mlflow, mlflow_sklearn, mlflow_client_cls = import_mlflow_dependencies()
    configure_mlflow_tracking(mlflow, settings)
    client = mlflow_client_cls()
    model_uri = build_registry_model_uri(settings)

    try:
        return mlflow_sklearn.load_model(model_uri)
    except Exception:
        latest_version = find_latest_model_version(client, settings.model_name)
        if latest_version is None:
            latest_version = register_model_in_mlflow(
                mlflow=mlflow,
                mlflow_sklearn=mlflow_sklearn,
                client=client,
                settings=settings,
            )
        set_model_alias(client, settings, latest_version)
        return mlflow_sklearn.load_model(model_uri)


def build_registry_model_uri(settings: ModelSettings) -> str:
    """Строит URI модели в MLflow Registry по alias."""
    return f"models:/{settings.model_name}@{settings.model_alias}"


def import_mlflow_dependencies():
    """Импортирует MLflow лениво, чтобы local mode не зависел от пакета."""
    try:
        mlflow = importlib.import_module("mlflow")
        mlflow_sklearn = importlib.import_module("mlflow.sklearn")
        tracking_module = importlib.import_module("mlflow.tracking")
    except ModuleNotFoundError as exc:
        raise RuntimeError("MLflow package is not installed") from exc

    return mlflow, mlflow_sklearn, tracking_module.MlflowClient


def configure_mlflow_tracking(mlflow: Any, settings: ModelSettings) -> None:
    """Настраивает tracking URI для MLflow клиента."""
    mlflow.set_tracking_uri(settings.tracking_uri)


def validate_mlflow_settings(settings: ModelSettings) -> None:
    """Проверяет обязательные настройки MLflow."""
    if not settings.tracking_uri:
        raise ValueError("MLFLOW_TRACKING_URI is required when USE_MLFLOW=true")


def find_latest_model_version(client: Any, model_name: str) -> str | None:
    """Возвращает последнюю версию зарегистрированной модели."""
    versions = client.search_model_versions(f"name = '{model_name}'")
    if not versions:
        return None
    latest = max(versions, key=lambda version: int(version.version))
    return str(latest.version)


def register_model_in_mlflow(
    mlflow: Any,
    mlflow_sklearn: Any,
    client: Any,
    settings: ModelSettings,
) -> str:
    """Обучает модель, логирует ее и возвращает версию в Model Registry."""
    mlflow.set_experiment(settings.experiment_name)
    model = train_model()

    with mlflow.start_run():
        model_info = mlflow_sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=settings.model_name,
        )

    registered_version = extract_registered_model_version(model_info)
    if registered_version is not None:
        return registered_version

    return wait_for_registered_model_version(client, settings.model_name)


def extract_registered_model_version(model_info: Any) -> str | None:
    """Извлекает номер версии из ответа log_model."""
    for attr_name in ("registered_model_version", "model_version", "version"):
        value = getattr(model_info, attr_name, None)
        if value is not None:
            return str(value)
    return None


def wait_for_registered_model_version(
    client: Any,
    model_name: str,
    timeout_seconds: float = 10.0,
    poll_interval_seconds: float = 0.2,
) -> str:
    """Ждет появления версии модели в registry после log_model."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        version = find_latest_model_version(client, model_name)
        if version is not None:
            return version
        time.sleep(poll_interval_seconds)
    raise RuntimeError(f"Registered model version was not created for '{model_name}'")


def set_model_alias(client: Any, settings: ModelSettings, version: str) -> None:
    """Привязывает alias к указанной версии модели."""
    client.set_registered_model_alias(
        settings.model_name,
        settings.model_alias,
        version,
    )
