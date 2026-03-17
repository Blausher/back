from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from app.clients import model as client_model
from app.services import model as model_service


def test_load_or_train_model_local_loads_existing_pickle(tmp_path):
    model_path = tmp_path / "model.pkl"
    expected_model = {"source": "disk"}
    model_service.save_model(expected_model, model_path)

    loaded_model = model_service.load_or_train_model(model_path, use_mlflow=False)

    assert loaded_model == expected_model


def test_load_or_train_model_local_trains_and_saves_when_file_is_missing(tmp_path, monkeypatch):
    model_path = tmp_path / "model.pkl"
    trained_model = {"source": "trained"}

    monkeypatch.setattr(model_service, "train_model", lambda: trained_model)

    loaded_model = model_service.load_or_train_model(model_path, use_mlflow=False)

    assert loaded_model == trained_model
    assert model_service.load_model(model_path) == trained_model


def test_load_or_train_model_mlflow_loads_model_by_alias(monkeypatch):
    expected_model = {"source": "mlflow"}
    load_calls = []

    class DummyMlflow:
        def __init__(self):
            self.tracking_uri = None

        def set_tracking_uri(self, uri):
            self.tracking_uri = uri

    class DummyMlflowSklearn:
        def load_model(self, model_uri):
            load_calls.append(model_uri)
            return expected_model

    class DummyClient:
        def search_model_versions(self, _filter_string):
            raise AssertionError("Registry lookup should not happen on direct alias hit")

        def set_registered_model_alias(self, _model_name, _alias, _version):
            raise AssertionError("Alias rebinding should not happen on direct alias hit")

    dummy_mlflow = DummyMlflow()
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
    monkeypatch.setattr(
        model_service,
        "import_mlflow_dependencies",
        lambda: (dummy_mlflow, DummyMlflowSklearn(), DummyClient),
    )

    loaded_model = model_service.load_or_train_model(use_mlflow=True)

    assert loaded_model == expected_model
    assert dummy_mlflow.tracking_uri == "sqlite:///mlflow.db"
    assert load_calls == ["models:/moderation-model@champion"]


def test_load_or_train_model_mlflow_rebinds_alias_to_latest_version(monkeypatch):
    expected_model = {"source": "registry"}
    load_calls = []
    alias_calls = []

    class DummyMlflow:
        def set_tracking_uri(self, _uri):
            return None

    class DummyMlflowSklearn:
        def load_model(self, model_uri):
            load_calls.append(model_uri)
            if len(load_calls) == 1:
                raise RuntimeError("Alias is not set")
            return expected_model

    class DummyClient:
        def search_model_versions(self, filter_string):
            assert filter_string == "name = 'moderation-model'"
            return [SimpleNamespace(version="2"), SimpleNamespace(version="10")]

        def set_registered_model_alias(self, model_name, alias, version):
            alias_calls.append((model_name, alias, version))

    monkeypatch.setenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
    monkeypatch.setattr(
        model_service,
        "import_mlflow_dependencies",
        lambda: (DummyMlflow(), DummyMlflowSklearn(), DummyClient),
    )
    monkeypatch.setattr(
        model_service,
        "train_model",
        lambda: (_ for _ in ()).throw(AssertionError("Training should not be called")),
    )

    loaded_model = model_service.load_or_train_model(use_mlflow=True)

    assert loaded_model == expected_model
    assert load_calls == [
        "models:/moderation-model@champion",
        "models:/moderation-model@champion",
    ]
    assert alias_calls == [("moderation-model", "champion", "10")]


def test_load_or_train_model_mlflow_registers_new_model_when_registry_is_empty(monkeypatch):
    expected_model = {"source": "registered"}
    trained_model = {"source": "trained"}
    load_calls = []
    alias_calls = []

    class DummyRun:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyMlflow:
        def __init__(self):
            self.tracking_uri = None
            self.experiment_name = None
            self.start_run_count = 0

        def set_tracking_uri(self, uri):
            self.tracking_uri = uri

        def set_experiment(self, experiment_name):
            self.experiment_name = experiment_name

        def start_run(self):
            self.start_run_count += 1
            return DummyRun()

    class DummyMlflowSklearn:
        def load_model(self, model_uri):
            load_calls.append(model_uri)
            if len(load_calls) == 1:
                raise RuntimeError("Alias is not set")
            return expected_model

        def log_model(self, model, artifact_path, registered_model_name):
            assert model == trained_model
            assert artifact_path == "model"
            assert registered_model_name == "moderation-model"
            return SimpleNamespace(registered_model_version="7")

    class DummyClient:
        def __init__(self):
            self.search_calls = []

        def search_model_versions(self, filter_string):
            self.search_calls.append(filter_string)
            return []

        def set_registered_model_alias(self, model_name, alias, version):
            alias_calls.append((model_name, alias, version))

    dummy_mlflow = DummyMlflow()
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
    monkeypatch.setattr(model_service, "train_model", lambda: trained_model)
    monkeypatch.setattr(
        model_service,
        "import_mlflow_dependencies",
        lambda: (dummy_mlflow, DummyMlflowSklearn(), DummyClient),
    )

    loaded_model = model_service.load_or_train_model(use_mlflow=True)

    assert loaded_model == expected_model
    assert dummy_mlflow.tracking_uri == "sqlite:///mlflow.db"
    assert dummy_mlflow.experiment_name == "moderation-model"
    assert dummy_mlflow.start_run_count == 1
    assert alias_calls == [("moderation-model", "champion", "7")]
    assert load_calls == [
        "models:/moderation-model@champion",
        "models:/moderation-model@champion",
    ]


def test_model_client_reads_use_mlflow_flag_from_env(monkeypatch):
    calls = []

    monkeypatch.setenv("USE_MLFLOW", "true")

    def fake_load_or_train_model(model_path, use_mlflow=None):
        calls.append((model_path, use_mlflow))
        return object()

    monkeypatch.setattr(client_model, "load_or_train_model", fake_load_or_train_model)
    client = client_model.ModelClient(model_path="custom.pkl")

    client.load()

    assert calls == [("custom.pkl", True)]


def test_model_client_wraps_loader_errors(monkeypatch):
    def fail_loader(_model_path, use_mlflow=None):
        raise ValueError("missing tracking uri")

    monkeypatch.setattr(client_model, "load_or_train_model", fail_loader)
    client = client_model.ModelClient(use_mlflow=True)

    with pytest.raises(client_model.ModelNotLoadedError):
        client.load()


def test_model_client_predict_probability_uses_loaded_model():
    class DummySklearnModel:
        def predict_proba(self, features):
            assert features.shape == (1, 4)
            return np.array([[0.2, 0.8]])

    advertisement = SimpleNamespace(
        is_verified_seller=True,
        images_qty=2,
        description="Text",
        category=3,
    )
    client = client_model.ModelClient(use_mlflow=False)
    client._model = DummySklearnModel()

    probability = client.predict_probability(advertisement)

    assert probability == 0.8


def test_load_or_train_model_mlflow_requires_tracking_uri(monkeypatch):
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.setattr(
        model_service,
        "import_mlflow_dependencies",
        lambda: (_ for _ in ()).throw(AssertionError("MLflow import should not happen")),
    )

    with pytest.raises(ValueError, match="MLFLOW_TRACKING_URI"):
        model_service.load_or_train_model(use_mlflow=True)
