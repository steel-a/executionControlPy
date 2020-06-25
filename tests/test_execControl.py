from executionControlPy import ExecutionControl
from dbpy.tests.connStringCfg import getConnStr
import dbpy.db_mysql as database
from executionControlPy.requirements import createDbObjects


def test_execControl():
    table = 'control'
    processName = 'getFinCBBRData'
    processParam = 'CBBR-ITCR;11753'

    connStr = getConnStr()
    db = database.DB(connStr)

    ec = ExecutionControl(connStr, table)

    createDbObjects.dropTableCtrl(db, table)
    createDbObjects.createTableCtrl(db,table)
    createDbObjects.createNewRegisterIfNE(db, table, 'getFinCDIData','',0)
    createDbObjects.createNewRegisterIfNE(db, table, 'getFinIPCAData','',0)
    createDbObjects.createNewRegisterIfNE(db, table, processName, processParam,0)

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

    createDbObjects.dropTableCtrl(db, table)


if __name__ == '__main__':
    test_execControl()