
class Transaction:
  # Id of transaction
  id : str = None
  # Shared Lock Array, list of Dataitem Name
  sharedLock: list[str] = None
  # Exclusive Lock Array, list of Dataitem Name
  exclusiveLock: list[str] = None


  def __init__(self, id: str, sl: list[str], xl : list[str]):
    self.id = id
    self.sharedLock = sl
    self.exclusiveLock = xl

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