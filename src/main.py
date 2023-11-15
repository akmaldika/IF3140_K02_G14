
if __name__ == "__main__":
  print("== Select concurrency control to be Implemented ==")
  print("1. Two-phase Locking (2PL)")
  print("2. Optimistic Concurrency Control (OCC)")
  print("3. Multiversion Timestamp Ordering Concurrency Control (MVCC)")

  userInput = 0
  while (userInput != 1 and userInput != 2 and userInput !=3):
    try:
      userInput = int(input("Choose an option: "))
      if (userInput < 1 or userInput > 3):
        print("Invalid input detected. Make sure you input the right thing!")
    except:
      print("Invalid input detected. Make sure you input the right thing!")
  
  print("Insert a transaction:")
  transaction = input()
  print(transaction)