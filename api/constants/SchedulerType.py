from enum import Enum

class SchedulerType(Enum):
    SGE = "SGE"
    HADOOP = "Hadoop"

    @classmethod
    def from_short(cls, short_code: str) -> "SchedulerType":
        '''
        Accepta codi curt ('S', 'H') i retorna l'enum corresponent.
        '''
        mapping = {
            "S": cls.SGE,
            "H": cls.HADOOP
        }
        return mapping.get(short_code.strip().upper())

    @classmethod
    def name_from_code(cls, code: str) -> str:
        '''
        Accepta codi curt ('S', 'H') i retorna el nom desplegat ('SGE' o 'Hadoop')
        '''
        scheduler = cls.from_short(code)
        if scheduler is None:
            return code  # o pots llançar ValueError si vols forçar validació
        return scheduler.value