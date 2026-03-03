from __future__ import annotations

from time import perf_counter
from typing import Protocol

import numpy as np

from app.observability.metrics import (
    MODEL_PREDICTION_PROBABILITY,
    PREDICTION_DURATION,
    PREDICTION_ERRORS_TOTAL,
)
from app.services.model import load_or_train_model


class ModelClientError(RuntimeError):
    """Базовая ошибка клиента ML-модели."""


class ModelNotLoadedError(ModelClientError):
    """Ошибка, когда модель не удалось загрузить."""


class ModelInferenceError(ModelClientError):
    """Ошибка инференса модели."""


class ModerationInput(Protocol):
    """Минимальный контракт данных для расчета признаков."""

    is_verified_seller: bool
    images_qty: int
    description: str
    category: int


class ModelClient:
    """Клиент работы с моделью: загрузка, нормализация признаков, инференс."""

    def __init__(self, model_path: str = "model.pkl") -> None:
        self.model_path = model_path
        self._model = None

    def load(self) -> None:
        """Загружает модель в память."""
        try:
            self._model = load_or_train_model(self.model_path)
        except Exception as exc:
            raise ModelNotLoadedError("Model is not loaded") from exc

    def _ensure_loaded(self):
        if self._model is None:
            self.load()
        if self._model is None:
            raise ModelNotLoadedError("Model is not loaded")
        return self._model

    @staticmethod
    def _build_features(item: ModerationInput) -> np.ndarray:
        """Нормализует входные данные в вектор признаков модели."""
        return np.array(
            [[
                1.0 if item.is_verified_seller else 0.0,
                min(item.images_qty, 10) / 10.0,
                len(item.description) / 1000.0,
                item.category / 100.0,
            ]],
            dtype=float,
        )

    def predict_probability(self, item: ModerationInput) -> float:
        """Возвращает вероятность класса `violation`."""
        try:
            model = self._ensure_loaded()
        except ModelNotLoadedError:
            PREDICTION_ERRORS_TOTAL.labels(error_type="model_unavailable").inc()
            raise

        features = self._build_features(item)
        started_at = perf_counter()
        try:
            probability = float(model.predict_proba(features)[0][1])
        except Exception as exc:
            PREDICTION_ERRORS_TOTAL.labels(error_type="prediction_error").inc()
            raise ModelInferenceError("Model inference failed") from exc
        finally:
            PREDICTION_DURATION.observe(perf_counter() - started_at)

        MODEL_PREDICTION_PROBABILITY.observe(probability)
        return probability
