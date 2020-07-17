import dbpy.db_mysql as database
from dbpy.db_mysql import f
from datetime import datetime
import re

class ExecutionControl:
    """
    -> Use instructions:
        Call getProcessToExec: load a candidate to be executed 
        Call start: try to checkout the candidate updating status to 'P' and verify return
            if True, process
            if success, call updateSuccess function
            if error, call updateError function
    """

    def __init__(self, conStr:str, table:str):
        self.table = table
        self.db = database.DB(conStr)


    def __enter__(self):
        return self


    def __exit__(self, type, value, tb):
        if tb is None:
            self.endSuccess()
        else:
            self.endError(value)


    def getProcessToExec(self, processName:str=None, processParam:str=None) -> bool:
        """
        -> loads a candidate to be executed and try to start.
            If it can't start, it will load another process.
            It loads values to self.* variables
        :return: True if load a candidate
                 False if does not have a candidate to load or error
        """

        if processName is None:
            queryCandidate = f"""
                select *
                from {self.table}
                where
                    (
                    (statusLastExecution = 'S'
                        /* Time rule */
                        and (hourStart <= date_format(now(),'%H:%i') or hourStart2 <= date_format(now(),'%H:%i')) and hourEnd >= date_format(now(),'%H:%i')
                        /* Specific rule for each periodicity */
                        and (
                            (periodicity = 'D' and dateLastSuccess < curdate()) or
                            (periodicity = 'B' and dateLastSuccess < curdate() and dayofweek(curdate()) between 2 and 6) or
                            (periodicity = 'W' and dateLastSuccess < curdate() and dayofweek(curdate()) = day) or
                            (periodicity = 'M' and dateLastSuccess < curdate() and ( (day > 0 and dayofmonth(curdate()) = day) or
                                                                                    (day = -1 and dayofmonth(curdate())=dayofmonth(last_day(curdate())))
                                                                                )) or
                            (periodicity = 'Y' and dateLastSuccess < curdate() and ( (day > 0 and dayofyear(curdate()) = day) or
                                                                                    (day = -1 and dayofyear(curdate())=dayofyear((DATE_ADD(NOW(), INTERVAL 12-MONTH(NOW()) MONTH))))
                                                                                )))
                        /* Execute if not executed today, or already exec today but needs to repeat every x minutes */
                        and (dateLastSuccess!=curdate() or (repeatMinutes>0 and timestampdiff(MINUTE,curdate(),now())>repeatMinutes))
                        )
                    or (statusLastExecution = 'E')
                    or (statusLastExecution = 'P' and timestampdiff(MINUTE,timeLastExecution,now())>30)
                    )
                    # Max retries
                    and (triesWithError < maxTriesWithError or timestampdiff(MINUTE,timeLastExecution,now())>minsAfterMaxTries)
                    order by idUser, preRequisites
                    LIMIT 1
                """

            for dic in self.db.getListRows(queryCandidate):
                
                preReqsAttended = True
                self.preRequisites = dic['preRequisites']
                if self.preRequisites!=None and self.preRequisites!='':
                    executedProcesses = ';'+self.db.getValuesSeparatedBy("SELECT ifnull(concat(fk,'-',idUser),ifnull(name,id)) FROM control where statusLastExecution = 'S' and dateLastSuccess>curdate();",';')+';'
                    for pre in self.preRequisites.split(';'):
                        if ';'+pre+';' not in executedProcesses:
                            preReqsAttended = False
                            break
                if not preReqsAttended: continue

                self.id = dic['id']
                self.name = dic['name']
                self.processName = dic['processName']
                self.processParam = dic['processParam']
                self.idUser = dic['idUser']
                self.periodicity = dic['periodicity']
                self.day = dic['day']
                self.hourStart = dic['hourStart']
                self.hourStart2 = dic['hourStart2']
                self.hourEnd = dic['hourEnd']
                self.repeatMinutes = dic['repeatMinutes']
                self.dateLastSuccess = dic['dateLastSuccess']
                self.statusLastExecution = dic['statusLastExecution']
                self.timeLastExecution = dic['timeLastExecution']
                self.triesWithError = dic['triesWithError']
                self.maxTriesWithError = dic['maxTriesWithError']
                self.minsAfterMaxTries = dic['minsAfterMaxTries']
                self.error = dic['error']
                self.numHardRegisters = dic['numHardRegisters']
                self.numHardRegistersLast = dic['numHardRegistersLast']
                self.numSoftRegisters = dic['numSoftRegisters']
                self.fk = dic['fk']

                return True
            return False

        else:
            self.id = 0
            self.name = ''
            self.processName = processName
            self.processParam = processParam
            self.idUser = 1
            self.periodicity = 'D'
            self.day = 0
            self.hourStart = '00:00'
            self.hourStart2 = None
            self.hourEnd = '24:00'
            self.repeatMinutes = 0
            #elf.dateLastSuccess = '2020-01-01 00:00:01'
            self.statusLastExecution = 'E'
            #self.timeLastExecution = '2020-01-01 00:00:01'
            self.triesWithError = 0
            self.maxTriesWithError = 3
            self.minsAfterMaxTries = 0
            self.error = ''
            self.numHardRegisters = 0
            self.numHardRegistersLast = 0
            self.numSoftRegisters = 0
            self.fk = ''

            return True


    def start(self, executionTime:str=None):
        if self.id==0: return True

        if executionTime is None:
            executionTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if self.db.exec(f"""
            update {self.table} set
                statusLastExecution = 'P'
            ,   timeLastExecution = '{executionTime}'
            where id = {self.id}
            """) > 0:
            self.statusLastExecution = 'P'
            self.timeLastExecution = executionTime
            return True
        return False


    def endSuccess(self, numHardRegisters:int=0, numSoftRegisters:int=0):
        if self.id==0:
            return True

        self.statusLastExecution = 'S'
        self.triesWithError = 0
        self.error = ''
        self.numHardRegistersLast = self.numHardRegisters
        self.numHardRegisters = numHardRegisters
        self.numSoftRegisters = numSoftRegisters
        mysql = f"""
            update {self.table}
            set statusLastExecution = '{self.statusLastExecution}'
            ,   dateLastSuccess = '{self.timeLastExecution}'
            ,   triesWithError = {self.triesWithError}
            ,   error = ''
            ,   numHardRegisters = {self.numHardRegisters}
            ,   numSoftRegisters = {self.numSoftRegisters}
            ,   numHardRegistersLast = {self.numHardRegistersLast}
            where id = {self.id}
            """
        self.db.reconnect()
        self.db.exec(mysql)


    def endError(self, errorMessage):
        self.statusLastExecution = 'E'
        self.triesWithError+=1
        self.error = str(errorMessage).replace("'","")
        self.numHardRegistersLast = self.numHardRegisters
        self.numHardRegisters = 0
        self.numSoftRegisters = 0

        mysql = f"""
            update {self.table}
            set statusLastExecution = '{self.statusLastExecution}'
            ,   triesWithError = {self.triesWithError}
            ,   error = '{self.error}'
            where id = {self.id}
            """
        self.db.reconnect()
        self.db.exec(mysql)



if __name__ == '__main__':
    from executionControlPy.tests.test_execControl import test_execControl
    test_execControl()