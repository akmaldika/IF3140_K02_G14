from model.DataItem import DataItem
from datetime import datetime
from model.Operation import Operation

class Transaction:
  # Id of transaction
  id : str = None
  # Shared Lock Array, list of Dataitem Name
  sharedLock: list[str] = None
  # Exclusive Lock Array, list of Dataitem Name
  exclusiveLock: list[str] = None

  readSet: list[DataItem] = None
  writeSet: list[DataItem] = None


  def __init__(self, id: str, sl: list[str], xl : list[str]):
    self.id = id
    self.sharedLock = sl
    self.exclusiveLock = xl
    self.readSet = []
    self.writeSet = []
    self.startTS = datetime.max
    self.validationTS = datetime.max
    self.finishTS = datetime.max

  def displayTransaction(self):
    print("| Transaction ID :", self.id)
    print("| Shared Lock : [ ", end='')
    for i in self.sharedLock:
      print(i+' ', end="")
    print("]")
    print("| Exclusive Lock : [ ", end='')
    for i in self.exclusiveLock:
      print(i+' ', end="")
    print("]")

  def addToReadSet(self, data_item: DataItem):
    if data_item not in self.readSet :
        self.readSet.append(data_item)
  
  def addToWriteSet(self, data_item: DataItem):
    if data_item not in self.writeSet :
        self.writeSet.append(data_item)

  def addToSet(self, operation: Operation):
    if operation.opType == "R":
      self.addToReadSet(operation.opDataItem)
    elif operation.opType == "W":
      self.addToWriteSet(operation.opDataItem)

  def getReadSet(self):
    item_list = []
    for data_item in self.readSet:
      item_list.append(data_item.name)

    return item_list
  
  def getWriteSet(self):
    item_list = []
    for data_item in self.writeSet:
      item_list.append(data_item.name)

    return item_list
  def markStartTS (self):
    self.startTS = datetime.now()

  def markValidationTS (self):
    self.validationTS = datetime.now()

  def markFinishTS (self):
    self.finishTS = datetime.now()
  
  def abort(self):
    self.startTS = datetime.max
    self.validationTS = datetime.max
    self.finishTS = datetime.max
    self.readSet = []
    self.writeSet = []
