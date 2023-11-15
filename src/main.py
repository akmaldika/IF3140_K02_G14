from utils.Parser import constructSchedule
from model.Schedule import Schedule
from datetime import datetime

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
  
  print()
  print("Insert a schedule:")
  print("e.g.: R-1(x); R-2(y); W-4(z)")
  scheduleInput = input()
  schedule = constructSchedule(scheduleInput)
  

  # # Hanya untuk testing
  # schedule.dataItemArr[0].rts = datetime(2022, 12, 28, 23, 55, 59, 342380)
  # schedule.displaySchedule()
  

  