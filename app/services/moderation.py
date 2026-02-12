from app.models.advertisement import Advertisement


class BusinessLogicError(RuntimeError):
    pass


def predict_has_violations(ad: Advertisement) -> bool:
    '''
    возвращает валидность объявления
    подтвержденные пользователи всегда публикуют валидные объявления
    неподтвержденные – невалидны
    '''
    return ad.is_verified_seller
