from models.advertisement import Advertisement


class BusinessLogicError(RuntimeError):
    pass


def predict_has_violations(ad: Advertisement) -> bool:
    '''
    подтвержденные пользователи всегда публикуют объявления без нарушений
    неподтвержденные – только при наличии изображений
    '''
    if ad.is_verified_seller:
        return False

    return ad.images_qty == 0
