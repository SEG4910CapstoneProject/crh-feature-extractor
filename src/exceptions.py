from typing import Optional


class DisposedException(Exception):
    def __init__(self, msg: Optional[str] = None):
        super().__init__(msg or "Attempted to use object that was already Disposed")