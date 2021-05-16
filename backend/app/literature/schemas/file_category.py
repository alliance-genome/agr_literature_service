from enum import Enum

class FileCategories(str, Enum):
    primary = 'primary'
    primary_figure = 'primary figure'
    supplemental = 'supplemental'
    supplemental_figure = 'supplemental_figure'
