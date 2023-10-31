from abc import abstractmethod


class BaseGenerator:
    def __init__(self, column_prefix=None):
        self.__column_prefix__ = column_prefix

    @abstractmethod
    def __generate_row__(self):
        ...

    def generate_row(self, *_args):
        prefix = self.__column_prefix__

        row_obj = self.__generate_row__()
        if self.__column_prefix__ is not None:
            for key, value in list(row_obj.items()):
                row_obj[f"{prefix}{key}"] = row_obj.pop(key)
        return row_obj
