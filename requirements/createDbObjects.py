import dbpy.db_mysql as database
from dbpy.db_mysql import f

def createTableCtrl(db, table:str):
    # Necessary table: getWebDataCtl
    # name (str 50): Name to identify the process
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
    CREATE TABLE IF NOT EXISTS {table} (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(50) DEFAULT NULL,
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
    `error` varchar(5000) DEFAULT NULL,
    `numHardRegisters` int(11) DEFAULT NULL,
    `numHardRegistersLast` int(11) DEFAULT NULL,
    `numSoftRegisters` int(11) DEFAULT NULL,
    `preRequisites` varchar(50) DEFAULT NULL,
    `fk` varchar(50) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `{table}_unique_index`  (`processName`
                                            ,`processParam`
                                            ,`idUser`
                                            ,`periodicity`
                                            ,`day`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """
    db.exec(mysql)


if __name__ == '__main__':
    from dbpy.tests.connStringCfg import getConnStr
    import re
    # try to get the first connectionString* from ./files/config/environment.ini
    try:
        r = re.findall(r'[ \t]*(connectionString)[a-zA-Z0-9 \t]*=[ \t]*([a-zA-Z0-9-+*\/=?!@#$%&()_{}\[\]<>:;,.~^`"\' ]*)[ \t]*',open('./files/config/environment.ini','r').read())
        connString = r[0][1]
    # If its not possible to get connectionString from file, get from dbpy test scripts
    except:
        connString = getConnStr()

    db = database.DB(connString)
    
    createTableCtrl(db,'control')
