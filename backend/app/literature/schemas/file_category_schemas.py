from enum import Enum

class FileCategories(str, Enum):
    primary = 'primary'
    primary_figure = 'primary_figure'
    supplemental = 'supplemental'
    supplemental_figure = 'supplemental_figure'
