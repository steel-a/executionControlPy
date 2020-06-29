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
    `id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name` varchar(50) DEFAULT NULL,
    `processName` varchar(50) DEFAULT NULL,
    `processParam` varchar(30) DEFAULT NULL,
    `idUser` SMALLINT UNSIGNED NOT NULL,
    `periodicity` char(1) NOT NULL,
    `day` SMALLINT unsigned DEFAULT NULL,
    `hourStart` char(5) NOT NULL,
    `hourStart2` char(5),
    `hourEnd` char(5) NOT NULL,
    `repeatMinutes` SMALLINT unsigned DEFAULT NULL,
    `dateLastSuccess` datetime DEFAULT NULL,
    `statusLastExecution` char(1) DEFAULT NULL,
    `timeLastExecution` datetime DEFAULT NULL,
    `triesWithError` TINYINT unsigned DEFAULT NULL,
    `maxTriesWithError` TINYINT unsigned DEFAULT NULL,
    `minsAfterMaxTries` TINYINT unsigned DEFAULT NULL,
    `error` varchar(5000) DEFAULT NULL,
    `numHardRegisters` SMALLINT unsigned DEFAULT NULL,
    `numHardRegistersLast` SMALLINT unsigned DEFAULT NULL,
    `numSoftRegisters` SMALLINT unsigned DEFAULT NULL,
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

def createNewRegisterIfNE(db, table:str, processName:str, processParam:str, idUser:int):

    num = db.getValue(f"""
        select count(1)
          from {table}
         where processName = {f(processName)}
           and processParam = {f(processParam)}
           and idUser = {f(idUser)}
        """)
    if num>0:
        return

    name = 'null'
    processName = processName
    processParam = processParam
    idUser = idUser
    periodicity = 'D'
    day = 'null'
    hourStart = '06:00'
    hourStart2 = 'null'
    hourEnd = '24:00'
    repeatMinutes = 0
    dateLastSuccess = '2000-01-01 00:00:01'
    statusLastExecution = 'E'
    timeLastExecution = '2000-01-01 00:00:01'
    triesWithError = 0
    maxTriesWithError = 3
    minsAfterMaxTries = 5
    error = 'null'
    numHardRegisters = 0
    numHardRegistersLast = 0
    numSoftRegisters = 0
    preRequisites = 'null'
    fk = 'null'

    mysql = f"""
        INSERT INTO {table}
        (
        `name`,
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
        {f(name)},
        {f(processName)},
        {f(processParam)},
        {f(idUser)},
        {f(periodicity)},
        {f(day)},
        {f(hourStart)},
        {f(hourStart2)},
        {f(hourEnd)},
        {f(repeatMinutes)},
        {f(dateLastSuccess)},
        {f(statusLastExecution)},
        {f(timeLastExecution)},
        {f(triesWithError)},
        {f(maxTriesWithError)},
        {f(minsAfterMaxTries)},
        {f(error)},
        {f(numHardRegisters)},
        {f(numHardRegistersLast)},
        {f(numSoftRegisters)},
        {f(preRequisites)},
        {f(fk)}
        );
        """.replace('\n','')

    db.exec(mysql)

def dropTableCtrl(db, table:str):
    db.exec(f"drop table if exists {table}")



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
