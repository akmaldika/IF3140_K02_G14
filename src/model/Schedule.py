from utils.Parser import *
from model.Operation import Operation
from model.DataItem import DataItem
from model.Transaction import Transaction


class Schedule:
  # Array of operation
  operationArr: list[Operation] = None
  # Array of data item used in Schedule
  dataItemArr: list[DataItem] = None
  # Array of all transaction
  transactionArr: list[Transaction] = None

  def __init__(self, operationArr: list, dataItemArr: list, transactionArr: list):
    self.operationArr = operationArr
    self.dataItemArr = dataItemArr
    self.transactionArr = transactionArr
  
  def displaySchedule(self):
    print("List of Operation:")
    for i in self.operationArr:
      i.displayOperation()
    print()

    print("List of Data Item")
    for i in self.dataItemArr:
      i.displayDataItem()
    print()

    print("List of Transaction")
    for i in self.transactionArr:
      i.displayTransaction()
    print()

  def printSchedule(self):
    # print("Schedule: ", end="")
    for i in self.operationArr:
      print(i.opName + "; ", end="")
    print()
  
  def dequeue(self) -> Operation:
    return self.operationArr.pop(0) 
  
  
    

