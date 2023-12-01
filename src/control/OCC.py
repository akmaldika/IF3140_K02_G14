from model.Schedule import Schedule
from datetime import datetime
from model.Operation import Operation
from model.Transaction import Transaction
from model.DataItem import DataItem
from utils.Printer import *


class OCC(Schedule):
    def __init__(self, schedule: Schedule):
        super().__init__(schedule.operationArr, schedule.dataItemArr, schedule.transactionArr)
        self.final_schedule = []
        self.run() 

    def get_Transaction(self, id: str):
        for transaction in self.transactionArr:
            if transaction.id == id:
                return transaction

    def validate(self, t: Transaction):
        valid = True
        for transaction in self.transactionArr:      
            if (transaction.validationTS < t.validationTS):
                condition1 = transaction.finishTS < t.startTS
                condition2 = True
                for item in transaction.writeSet :
                    condition2 = condition2 and (item not in t.readSet)

                if not (condition1 or condition2) :       # Validation fail
                    valid = False
        return valid

    def execute_operation(self, operation: Operation):
        transaction = self.get_Transaction(operation.opTransaction)
        transaction.addToSet(operation)
        description = []
        if operation.opType == "R":
            self.final_schedule.append(operation)
            description.append(f"Read Set : {transaction.getReadSet()}")
            print_process(operation.opType, operation.opTransaction, operation.opDataItem.name, description)

        elif operation.opType == "W":
            self.final_schedule.append(Operation("TW", transaction.id, operation.opDataItem))
            description.append(f"Write Set : {transaction.getWriteSet()}")
            print_process("TW", operation.opTransaction, operation.opDataItem.name, description)

        elif operation.opType == "C":
            self.final_schedule.append(Operation("V", transaction.id, None))
            print_process("<validate>", operation.opTransaction, "", [])
            transaction.markValidationTS()
            valid = self.validate(transaction)
            if (valid):
                # Write Phase
                for item in transaction.writeSet:
                    print_process("W", transaction.id, item.name, [])
                    self.final_schedule.append(Operation("W", transaction.id, item))

                print_process(operation.opType, transaction.id, "", [])
                transaction.markFinishTS()
                self.final_schedule.append(operation)
            else :
                self.final_schedule.append(Operation("A", transaction.id, None))
                print_process("Abort", operation.opTransaction, "", [f"T{transaction.id} rolled back"])
                transaction.abort()
                self.run_all(transaction.id)
            return

        if transaction.startTS == datetime.max:
            transaction.markStartTS()

    def run(self):
        print_header()
        for operation in self.operationArr:
            self.execute_operation(operation)   
        
        print_result(self.final_schedule)

    def run_all(self, transactionID: str):
        for operation in self.operationArr:
            if operation.opTransaction == transactionID:
                self.execute_operation(operation)
