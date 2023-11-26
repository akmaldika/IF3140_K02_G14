from model.DataItem import *

class Operation:
    # Operation Type
    opType: str = None
    '''
    R : Read
    W : Write
    C : Commit
    A : Abort
    SL : Grant Shared Lock
    XL : Grant Exclusive Lock
    UL : Unlock
    '''
    # Operation Transaction
    opTransaction: str = None
    # Operation Data Item
    opDataItem: DataItem = None
    # Operation Display Name
    opName: str = None

    def __init__(self, opType: str, opTransaction: str, opDataItem: DataItem):
      self.opType = opType
      self.opTransaction = opTransaction
      self.opDataItem = opDataItem
      self.opName = opType + "-" + opTransaction
      if (opDataItem != None):
         self.opName += "(" + self.opDataItem.name + ")"
      
    
    def displayOperation(self, indent= 0):
       print("  " * indent + "| Operation Type :", self.opType)
       print("  " * indent + "| Operation Transaction :", self.opTransaction)
       print("  " * indent + "| Operation Data Item :")
       print("{")
       self.opDataItem.displayDataItem(1) if self.opDataItem != None else print("  " * (indent+1) + "| None")
       print("}")