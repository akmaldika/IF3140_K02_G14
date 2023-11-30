from model.Schedule import Schedule
from model.Operation import Operation

class TwoPL(Schedule):
  # Waiting queue
  waitingList: list[Operation] = list()
  prevWaitingList: list[Operation] = list()
  # Final schedule
  result: list[Operation] = list()
  lockTable: list[Operation] = list()

  def __init__(self, schedule: Schedule):
    super().__init__(schedule.operationArr, schedule.dataItemArr, schedule.transactionArr)
    self.run()

  def displayWaitingList(self):
    for i in self.waitingList:
      print(i.opName + "; ", end="")
    print()
  def displayLock(self):
    for i in self.lockTable:
      print(i.opName + "; ", end="")
    print()
  def displayResult(self):
    for i in self.result:
      print(i.opName + "; ", end="")
    print()

  def display(self, currOp: Operation):
    print("Current Operation: " + currOp.opName)
    print("Schedule : ", end="")
    self.printSchedule()
    print("Waiting List: ", end="")  
    self.displayWaitingList()
    print("Lock tables :", end="")
    self.displayLock()
    print("Result : ", end="")
    self.displayResult()
    print()
      
  def setSLock(self, op: Operation) -> bool:
    sLock = Operation("SL", op.opTransaction, op.opDataItem)
    xLock = Operation("XL", op.opTransaction, op.opDataItem)
    # cek apakah current transaction telah memegang share/exclusive lock
    if (self.isInTableLock(sLock)) or (self.isInTableLock(xLock)):
      return True
    # cek apakah ada transaksi lain yang sedang memegang exclusive lock
    if self.hasExclusiveLock(op):
      return False
    # set lock
    self.lockTable.append(sLock)
    self.result.append(sLock)
    return True
  
  def setXLock(self, op: Operation) -> bool:
    # cek apakah sudah memegang XLock
    xLock =  Operation("XL", op.opTransaction, op.opDataItem)
    sLock =  Operation("SL", op.opTransaction, op.opDataItem)
    if (self.isInTableLock(xLock)):
      return True
    # cek apakah ada transaksi lain yang sedang memegang exclusive lock
    if self.hasExclusiveLock(op):
      return False
    # cek apakah ada transaksi yang shared lock
    if self.hasSharedLock(op):
      return False
    # cek apakah upgrade
    if (self.isInTableLock(sLock)):
      # remove shared lock
      for lock in self.lockTable:
        if (lock.opDataItem.name == sLock.opDataItem.name):
          self.lockTable.remove(lock)
    # set lock
    self.lockTable.append(xLock)
    self.result.append(xLock)
    return True
    
  def commit(self, op: Operation):
    # cek apakah di waiting list masih ada transaksinya 
    for w in self.waitingList:
      if w.opTransaction == op.opTransaction and w.opType != op.opType:
        return False
    # remove lock
    unlock = []
    for lock in self.lockTable:
      if lock.opTransaction == op.opTransaction:
        unlock.append(Operation("UL", lock.opTransaction, lock.opDataItem))
    self.lockTable = [lock for lock in self.lockTable if lock.opTransaction != op.opTransaction]
    return unlock
    
  def hasExclusiveLock(self, lock: Operation) -> bool:
    for l in self.lockTable:
      if (lock.opDataItem.name == l.opDataItem.name) and l.opType == "XL" and lock.opTransaction != l.opTransaction:
        return True
  
  def hasSharedLock(self, lock: Operation) -> bool:
    for l in self.lockTable:
      if (lock.opDataItem.name == l.opDataItem.name) and l.opType == "SL" and lock.opTransaction != l.opTransaction:
        return True
      
  def isInTableLock(self, lock: Operation) -> bool:
    for l in self.lockTable:
      if (lock.opDataItem.name == l.opDataItem.name) and (lock.opTransaction == l.opTransaction) and (lock.opType == l.opType):
        return True
      
  def process(self, currOp: Operation, isWaitingList: bool) -> bool:
    isSuccess = False
    match currOp.opType:
      case "R":
        isSuccess = self.setSLock(currOp)
      case "W":
        isSuccess = self.setXLock(currOp)
      case "C":
        isSuccess = self.commit(currOp)
        
    if (not isWaitingList):    
      if (isSuccess):
        self.result.append(currOp)
        if type(isSuccess) != bool:
          self.result += isSuccess
      else:
        self.prevWaitingList = self.waitingList.copy()
        self.waitingList.append(currOp)
      self.display(currOp)
    else:
      if (isSuccess):
        self.result.append(currOp)
        if type(isSuccess) != bool:
          self.result += isSuccess
        self.waitingList.pop(0)
        self.display(currOp)
    return isSuccess

  def isTscInWaitingList(self, currOp: Operation):
    for op in self.waitingList:
      if op.opTransaction == currOp.opTransaction:
        self.prevWaitingList = self.waitingList.copy()
        self.waitingList.append(currOp)
        return True
    

# W-1(X); W-2(Y); W-1(Y); W-2(X); C-1; C-2 -> deadlock
# R-1(X); R-2(X); W-2(Y); W-3(Y); W-1(X); C-1; C-2; C-3
  def run(self):
    print()
    while (len(self.operationArr) != 0) or (len(self.waitingList) != 0):
      # if the schedule already empty but the waiting list is not
      if (len(self.operationArr) == 0) and (len(self.waitingList) != 0):
        if self.waitingList == self.prevWaitingList:
          print("deadlock detected")
          break
      # process the waiting list first
      isSuccess = True
      while (isSuccess and len(self.waitingList) != 0):
        isSuccess = self.process(self.waitingList[0], True)
      
      # process schedule
      if (len(self.operationArr) != 0):
        currOp= self.dequeue()
        if (not self.isTscInWaitingList(currOp)):
          self.process(currOp, False)