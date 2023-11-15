from model.Schedule import Schedule
from model.Operation import Operation
from model.DataItem import DataItem
from model.Transaction import Transaction

from datetime import datetime as dt

def constructSchedule(sched: str) -> Schedule:
    operationArr = []
    dataItemArr = []
    transactionArr = []

    # Parse Operation
    sched = sched.replace(" ","")
    arr = sched.split(";")
    for i in range(len(arr)):
      if (len(arr[i]) == 0):
        arr.pop(i)
      else:
        tempOp = parseOperation(arr[i])
        currentOpType = tempOp[0]
        currentOpTransaction = tempOp[1]
        currentOpDataItem = tempOp[2]

        tempTransaction = getTransactionFromArray(transactionArr, currentOpTransaction)
        if (tempTransaction == None):
          # If there is None, create a new one
          tempTransaction = Transaction(currentOpTransaction, [], [])
          transactionArr.append(tempTransaction)

        if (currentOpDataItem != None):
          # there is Data Item : e.g. R-1(x)
          tempDataItem = getDataItemFromArray(dataItemArr, currentOpDataItem)
          if (tempDataItem == None):
            # If there is None, then create a new one
            tempDataItem = DataItem(currentOpDataItem, dt.now(), dt.now())
            dataItemArr.append(tempDataItem)
        else :
          # there is no Data Item : e.g. C-1
          tempDataItem = None


        operation = Operation(currentOpType, currentOpTransaction, tempDataItem)
        operationArr.append(operation)

    schedule = Schedule(operationArr, dataItemArr, transactionArr)
    return schedule

def parseOperation(op: str) -> list:
  temp = op.split("-")
  opType = temp[0]

  nextTemp = temp[1].split("(")
  opTransaction = nextTemp[0]

  if (len(nextTemp) > 1):
    # there is Data Item : e.g. R-1(x)
    opDataItem = nextTemp[1].replace(")", "")
  else :
    # there is no Data Item : e.g. C-1
    opDataItem = None
  return [opType, opTransaction, opDataItem]

def getDataItemFromArray(arr: list[DataItem], name: str) -> DataItem:
  for i in (arr):
    if (i.name == name):
       return i
  return None

def getTransactionFromArray(arr: list[Transaction], id: str) -> Transaction:
  for i in (arr):
    if (i.id == id):
      return i
  return None