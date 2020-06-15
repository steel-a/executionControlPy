from executionControlPy import ExecutionControl
from dbpy.tests.connStringCfg import getConnStr
import dbpy.db_mysql as database
from dbpy.db_mysql import f

class ExecutionControlTest:

    def __init__(self, conStr:str, table:str):
        self.table = table
        self.db = database.DB(conStr)


    def _createNewRegister(self, processName:str, processParam:str, idUser:int):
        self.processName = processName
        self.processParam = processParam
        self.idUser = idUser
        self.periodicity = 'D'
        self.day = 'null'
        self.hourStart = '00:00'
        self.hourStart2 = 'null'
        self.hourEnd = '24:00'
        self.repeatMinutes = 0
        self.dateLastSuccess = '2000-01-01 00:00:01'
        self.statusLastExecution = 'E'
        self.timeLastExecution = '2000-01-01 00:00:01'
        self.triesWithError = 0
        self.maxTriesWithError = 3
        self.minsAfterMaxTries = 5
        self.error = 'null'
        self.numHardRegisters = 0
        self.numHardRegistersLast = 0
        self.numSoftRegisters = 0
        self.preRequisites = 'null'
        self.fk = 'null'

        mysql = f"""
            INSERT INTO {self.table}
            (
            `processName`,
            `processParam`,
            `idUser`,
            `periodicity`,
            `day`,
            `hourStart`,
            `hourStart2`,
            `hourEnd`,
            `repeatMinutes`,
            `dateLastSuccess`,
            `statusLastExecution`,
            `timeLastExecution`,
            `triesWithError`,
            `maxTriesWithError`,
            `minsAfterMaxTries`,
            `error`,
            `numHardRegisters`,
            `numHardRegistersLast`,
            `numSoftRegisters`,
            `preRequisites`,
            `fk`
            )
            VALUES
            (
            {f(self.processName)},
            {f(self.processParam)},
            {f(self.idUser)},
            {f(self.periodicity)},
            {f(self.day)},
            {f(self.hourStart)},
            {f(self.hourStart2)},
            {f(self.hourEnd)},
            {f(self.repeatMinutes)},
            {f(self.dateLastSuccess)},
            {f(self.statusLastExecution)},
            {f(self.timeLastExecution)},
            {f(self.triesWithError)},
            {f(self.maxTriesWithError)},
            {f(self.minsAfterMaxTries)},
            {f(self.error)},
            {f(self.numHardRegisters)},
            {f(self.numHardRegistersLast)},
            {f(self.numSoftRegisters)},
            {f(self.preRequisites)},
            {f(self.fk)}
            );
            """.replace('\n','')

        self.db.exec(mysql)


    def _dropTableCtrl(self):
        self.db.exec(f"drop table if exists {self.table}")


    def _createTableCtrl(self):
        # Necessary table: getWebDataCtl
        # ProcessName (str 50): Process Name
        # ProcessParam (str 30): Process Param
        # Periodicity (str 1) D (diary), W (weekly), M (monthly),
        #          B (business day diary but not saturday or sunday)
        # Day (int): Day number - 1 (Monday) to 7 (Sunsay)
        #            for Weekly or month day number for M
        # HourStart: Hour to first try
        # HourStart2: Hour to first try
        # HourEnd: Not to execute after this hour
        # RepeatMinutes: After start in this day, repeat every x min
        # DateLastSuccess (date): Last time exec result in Success
        # StatusLastExecution (str 1): S, P, E
        # TimeLastExecution (str 1): S, P, E
        # TriesWithError: int
        # MaxTriesWithError: int
        # minsAfterMaxTries: int
        # Error (str 1000): Last Error Message
        # NumHardRegisters
        # NumHardRegistersLast
        # NumSoftRegisters
        # preRequisites: list of ids that must be executed today
        # fk: field to relation to other database tables

        mysql =f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `processName` varchar(50) DEFAULT NULL,
        `processParam` varchar(30) DEFAULT NULL,
        `idUser` int NOT NULL,
        `periodicity` char(1) NOT NULL,
        `day` tinyint(4) DEFAULT NULL,
        `hourStart` char(5) NOT NULL,
        `hourStart2` char(5),
        `hourEnd` char(5) NOT NULL,
        `repeatMinutes` tinyint(4) DEFAULT NULL,
        `dateLastSuccess` datetime DEFAULT NULL,
        `statusLastExecution` char(1) DEFAULT NULL,
        `timeLastExecution` datetime DEFAULT NULL,
        `triesWithError` tinyint(4) DEFAULT NULL,
        `maxTriesWithError` tinyint(4) DEFAULT NULL,
        `minsAfterMaxTries` tinyint(4) DEFAULT NULL,
        `error` varchar(1000) DEFAULT NULL,
        `numHardRegisters` int(11) DEFAULT NULL,
        `numHardRegistersLast` int(11) DEFAULT NULL,
        `numSoftRegisters` int(11) DEFAULT NULL,
        `preRequisites` varchar(50) DEFAULT NULL,
        `fk` varchar(50) DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `{self.table}_unique_index`  (`processName`
                                                ,`processParam`
                                                ,`idUser`
                                                ,`periodicity`
                                                ,`day`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """
        self.db.exec(mysql)


def test_execControl():
    table = 'control'
    processName = 'getCBBRData'
    processParam = 'CBBR-ITCR;11753'

    connStr = getConnStr()
    ect = ExecutionControlTest(connStr, table)
    ec = ExecutionControl(connStr, table)

    ect._dropTableCtrl()
    ect._createTableCtrl()
    ect._createNewRegister('getCDIData','',0)
    ect._createNewRegister('getIPCAData','',0)
    ect._createNewRegister(processName, processParam,0)

    assert ec.getProcessToExec() != False
    assert ec.start() == True
    #assert ec.processName == processName
    #assert ec.processParam == processParam
    assert ec.id == 1
    assert ec.statusLastExecution == 'P'
    assert ec.triesWithError == 0
    ec.endError('Er mess')
    assert ec.statusLastExecution == 'E'
    assert ec.triesWithError == 1
    assert ec.getProcessToExec() != False
    assert ec.start() == True
    assert ec.statusLastExecution == 'P'
    assert ec.triesWithError == 1
    assert ec.error == 'Er mess'
    ec.endError('Er mess2')
    assert ec.statusLastExecution == 'E'
    assert ec.triesWithError == 2
    assert ec.getProcessToExec() != False
    assert ec.start() == True
    assert ec.statusLastExecution == 'P'
    assert ec.triesWithError == 2
    assert ec.error == 'Er mess2'
    ec.endSuccess(5,7)
    assert ec.statusLastExecution == 'S'
    assert ec.triesWithError == 0
    assert ec.error == ''
    assert ec.numHardRegistersLast == 0
    assert ec.numHardRegisters == 5
    assert ec.numSoftRegisters == 7

    ect._dropTableCtrl()


if __name__ == '__main__':
    test_execControl()