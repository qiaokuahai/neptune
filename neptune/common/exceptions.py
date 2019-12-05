
class BaseException(Exception):
    def __init__(self):
        self._code = 500
        self._message = "internal server error"

    @property
    def message(self):
        return self._message

    @property
    def code(self):
        return self._code


class ValidateException(BaseException):
    def __init__(self, message="参数校验异常", code=400):
        self._code = code
        self._message = message

    @property
    def message(self):
        return self._message

    @property
    def code(self):
        return self._code


class NotFindException(BaseException):
    def __init__(self, message="资源不存在", code=404):
        self._code = code
        self._message = message

    @property
    def message(self):
        return self._message

    @property
    def code(self):
        return self._code
