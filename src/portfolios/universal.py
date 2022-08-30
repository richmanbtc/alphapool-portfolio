from universal.algos import UP
from .base.up_base import UpBase


class Universal(UpBase):
    def __init__(self):
        super().__init__(UP())
