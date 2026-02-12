class AppError(Exception):
    """Базовая ошибка приложения."""


class StorageError(AppError):
    """Базовая ошибка слоя хранения данных."""


class StorageUnavailableError(StorageError):
    """Слой хранения недоступен или вернул неожиданную ошибку."""


class UserAlreadyExistsError(StorageError):
    """Пользователь уже существует."""


class SellerNotFoundError(StorageError):
    """Продавец не найден."""


class AdvertisementAlreadyExistsError(StorageError):
    """Объявление уже существует."""
