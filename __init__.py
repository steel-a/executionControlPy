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


    def getProcessToExec(self, processName:str=None) -> bool:
        """
        -> loads a candidate to be executed and try to start.
            If it can't start, it will load another process.
            It loads values to self.* variables
        :return: True if load a candidate
                 False if does not have a candidate to load or error
        """
        hourMin = re.search(' ([0-9]{2}:[0-9]{2}):[0-9]{2}',
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'))[1]

        if processName is None:
            queryCandidate = f"""
                select *
                from {self.table}
                where
                (statusLastExecution != 'S'
                    and (hourStart <= '{hourMin}' or hourStart2 <= '{hourMin}')
                    and     hourEnd >= '{hourMin}'
                ) or
                (statusLastExecution = 'E')
                """
        else:
            queryCandidate = f"""
                select * from {self.table} where statusLastExecution != 'P'
                and processName = '{processName}'
                """


        for dic in self.db.getListRows(queryCandidate):
            if dic is None:
                return False

            # Rules to not execute
            if processName==None:
                p = dic['periodicity']
                today = datetime.now()
                dtSuccess = dic['dateLastSuccess']
                dtExec = dic['timeLastExecution']
                minSinceSuccess = round((today-dtSuccess).seconds/60)
                minSinceExec = round((today-dtExec).seconds/60)
                repeat = dic['repeatMinutes']
                status = dic['statusLastExecution']
                dayWeek = today.weekday()+1
                businessDay = (dayWeek <= 5)
                executedToday = (today.strftime('%Y-%m-%d') == dtSuccess.strftime('%Y-%m-%d'))
                day = dic['day']
                tries = dic['triesWithError']
                maxTries = dic['maxTriesWithError']
                minsAfterTries = dic['minsAfterMaxTries']

                # Repeat = 0 -> execute once a day      # Repeat > 0 -> exec even x minutes
                if((repeat == 0 and executedToday) or (repeat > 0 and (minSinceSuccess < repeat))):
                    continue

                # Periodicity criteria
                elif status == 'S':
                    if ((p=='B' and not businessDay)
                    or (p=='W' and day != dayWeek)
                    or (p=='M' and day != today.date.day)
                    ):
                        continue

                # After maxTries, exec every 30 minutes
                elif status == 'E' and tries >= maxTries and minSinceExec <= minsAfterTries:
                    continue

            #Verify preRequisites
            queryMetRequisites = f"""
                select id from {self.table}
                 where dateLastSuccess >= '{datetime.now().strftime('%Y-%m-%d')+' 00:00:01'}'
                   and statusLastExecution = 'S'
            """
            lstMetRequisites = self.db.fetchall(queryMetRequisites)
            self.preRequisites = dic['preRequisites']
            if self.preRequisites is not None:
                allReqMet = True
                for idReq in self.preRequisites.split(';'):
                    if idReq not in lstMetRequisites:
                        allReqMet = False
                if not allReqMet:
                    continue
            

            self.id = dic['id']
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


    def start(self, executionTime:str=None):
        if executionTime is None:
            executionTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if self.db.exec(f"""
            update {self.table} set
                statusLastExecution = 'P'
            ,   timeLastExecution = '{executionTime}'
            where statusLastExecution != 'P' and id = {self.id}
            """) > 0:
            self.statusLastExecution = 'P'
            self.timeLastExecution = executionTime
            return True
        return False


    def endSuccess(self, numHardRegisters:int=0, numSoftRegisters:int=0):
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
            ,   numHardRegisters = {self.numHardRegisters}
            ,   numSoftRegisters = {self.numSoftRegisters}
            ,   numHardRegistersLast = {self.numHardRegistersLast}
            where id = {self.id}
            """
        self.db.reconnect()
        self.db.exec(mysql)



if __name__ == '__main__':
    from executionControlPy.tests.test_execControl import test_execControl
    test_execControl()