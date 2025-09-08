from enum import Enum

class SchedulerType(Enum):
    SGE = "SGE"
    HADOOP = "Hadoop"

    @classmethod
    def from_short(cls, short_code: str) -> "SchedulerType":
        """
        Accepts a short code ('S', 'H') and returns the corresponding enum value.
        """
        mapping = {
            "S": cls.SGE,
            "H": cls.HADOOP
        }
        return mapping.get(short_code.strip().upper())

    @classmethod
    def name_from_code(cls, code: str) -> str:
        """
        Accepts a short code ('S', 'H') and returns the full scheduler name ('SGE' or 'Hadoop').
        """
        scheduler = cls.from_short(code)
        if scheduler is None:
            return code
        return scheduler.value