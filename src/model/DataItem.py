from datetime import datetime

class DataItem:
  # Name of DataItem
  name: str = None
  # TimeStamp for Read
  rts: datetime = None
  # TimeStamp for Write
  wts: datetime = None

  def __init__(self, name:str, rts: datetime, wts: datetime):
    self.name = name
    self.rts = rts
    self.wts = wts

  def displayDataItem(self, indent = 0):
    print("  " * indent + "| Data Item Name :", self.name)
    print("  " * indent + "| Read Time Stamp :", self.rts)
    print("  " * indent + "| Write Time Stamp :", self.wts)

  def __str__(self):
        return str(self.name)