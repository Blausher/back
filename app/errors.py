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


class AdvertisementNotFoundError(AppError):
    """Объявление не найдено."""


class AccountAlreadyExistsError(StorageError):
    """Аккаунт уже существует."""


class ModerationTaskNotFoundError(AppError):
    """Задача модерации не найдена."""


class AuthenticationError(AppError):
    """Базовая ошибка авторизации."""


class InvalidCredentialsError(AuthenticationError):
    """Логин или пароль невалидны."""


class AccountBlockedError(AuthenticationError):
    """Аккаунт заблокирован."""


class InvalidTokenError(AuthenticationError):
    """JWT-токен невалиден."""
