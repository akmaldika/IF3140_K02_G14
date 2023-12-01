from model.DataItem import DataItem
from model.Operation import Operation
from model.Transaction import Transaction
from model.Schedule import Schedule

from utils.Printer import print_process, print_header, print_result, print_title

from dataclasses import dataclass, field
from typing import Dict

@dataclass
class DataTimestamp:
    """
    DataTimestamp merupakan class yang menyimpan timestamp pembacaan dan penulisan sebuah data
    """    
    ts_r: int
    ts_w: int

@dataclass
class MVCCDataItem:
    """
    MVCCDataItem merupakan class yang menyimpan data dan versi dari data tersebut
    """
    data: str
    versions: Dict[str, DataTimestamp] = field(default_factory=dict)

    def __post_init__(self):
        # Inisialisasi versi 0
        self.add_version(0)

    def add_version(self, version: str) -> None:
        if version not in self.versions:
            self.versions[version] = DataTimestamp(ts_r=0, ts_w=0)

@dataclass
class MVCCTransaction:
    """
    MVCCTransaction merupakan class yang menyimpan informasi mengenai sebuah transaksi
    """
    transaction: Transaction
    ts: int = 0
    
    def __str__(self) -> str:
        return f"{self.transaction.id} TS: {self.ts}"
    
class MVCC:
    """
    MVCC merupakan class yang mengimplementasikan algoritma MVCC
    """
    # Data ("X", "Y", dll.) yang memiliki banyak versi dengan timestamp read dan write
    data_items: Dict[str, MVCCDataItem] = field(default_factory=dict)
    
    # transaction yang ada pada schedule
    transactions: Dict[str, MVCCTransaction] = field(default_factory=dict)
    
    # queue operasi yang akan dijalankan | operation
    ope_queue: list[Operation] = list()
    
    # aborted queue operasi yang akan dimasukan ketika abort | operation
    aborted_queue: list[Operation] = list()
    
    """ 
    result queue operasi yang telah dijalankan | operation, ts saat operasi dijalankan, dan versi, log concurency control
    log :
    read:
    - ver: X0 ts_r(num) < TS(num Ti), TS(X0) = (ts_r, ts_w)
    write:
    if TS(Ti) < tr_r(Qk) then abort Ti
    - {ope.dataItem.name} ver
    """
    result_queue: list[tuple[Operation, int, str, str]] = list()
    
    # dependency graph in matrix (id_transaction1, id_transaction2) artinya id_transaction2 bergantung pada id_transaction1 (misal write-1 lalu read-2)
    dependency_graph: list[tuple[str, str]] = field(default_factory=list)
    
    
    
    def __init__(self, schedule: Schedule):
        self.schedule = schedule
        self.ope_queue = schedule.operationArr.copy()
            
        self.data_items = {
            data_item.name: MVCCDataItem(data=data_item.name)
            for data_item in schedule.dataItemArr
        }
            
        self.transactions = {
            transaction.id: MVCCTransaction(transaction=transaction, ts=i + 1)
            for i, transaction in enumerate(schedule.transactionArr)
        }
        
        print_title("MULTIVERSION TIMESTAMP ORDERING CONCURRENCY CONTROL (MVCC)")
        self.run()
        print_header()
        for element in self.result_queue:
            print_process(element[0].opType, element[0].opTransaction, element[0].opDataItem.name, [element[3]])

    
    def _is_ope_queue_empty(self) -> bool:
        return len(self.ope_queue) == 0
    
    def _is_aborted_queue_empty(self) -> bool:
        return len(self.aborted_queue) == 0
    
    def _get_largest_ts(self, ope: Operation, ts: int) -> int:
        """
        Get largest timestamp less than or equal to ts
        """
        data_item: MVCCDataItem = self.data_items[ope.opDataItem.name]
        
        relevant_versions = [
            version
            for version, timestamp in data_item.versions.items()
            if timestamp.ts_w <= ts and ope.opDataItem.name == data_item.data
        ]
        
        if relevant_versions:
            return max(relevant_versions)
        else:
            return 0
    
    def _process_read(self, op: Operation, ts: int) -> bool:
        """ 
        If transaction Ti issues a read(Q), then the value returned is the  content of version Qk; If R-timestamp(Qk) < TS(Ti), set R-timestamp(Qk) = TS(Ti)
        penjelasan:
        1. Cari versi data item pada list data_items dengan nama data item == ope.dataItem.name dan timestamp write terbesar <= ts, sebut Qk
        2. update timestamp read dengan ts jika timestamp read < ts
        
        """

        latest_version = self._get_largest_ts(op, ts)
        # ver: X0 ts_r(num) < TS(num Ti), TS(X0) = (ts_r, ts_w)
        
        if latest_version + 1:
            # Perbarui timestamp read pada data_item
            # print("Read <")
            self.data_items[op.opDataItem.name].versions[latest_version].ts_r = ts
            
            log = f"ver: {op.opDataItem.name}{latest_version} R-ts({self.data_items[op.opDataItem.name].versions[latest_version].ts_r - ts}) < TS({self.transactions[op.opTransaction].ts}) TS({op.opDataItem.name}{latest_version}) = ({self.data_items[op.opDataItem.name].versions[latest_version].ts_r}, {self.data_items[op.opDataItem.name].versions[latest_version].ts_w})"
            
        else:
            # print("Read >=")
            log = f"ver: {op.opDataItem.name}{latest_version} R-ts({self.data_items[op.opDataItem.name].versions[latest_version].ts_r}) >= TS({self.transactions[op.opTransaction].ts}) TS({op.opDataItem.name}{latest_version}) = ({self.data_items[op.opDataItem.name].versions[latest_version].ts_r}, {self.data_items[op.opDataItem.name].versions[latest_version].ts_w})"
        
        # update result queue
        self.result_queue.append((op, ts, latest_version, log))
        
        # dipastikan selalu berhasil
        return True

    def _process_write(self, op: Operation, ts: int) -> bool:
        """
        If transaction Ti issues a write(Q)
        1. if TS(Ti) < R-timestamp(Qk), then transaction Ti is rolled back. 
        2. if TS(Ti) = W-timestamp(Qk), the contents of Qk are overwritten
        3. Otherwise,  a new version Qi of Q is created
            - W-timestamp(Qi) and R-timestamp(Qi) are initialized to TS(Ti). 
        
        penjelasan
        1. Cari versi data item pada list data_items dengan nama data_item.data == ope.dataItem.name dan timestamp write terbesar <= ts, sebut Qk
        2. lakukan:
            - jika ts < timestamp read Qk, maka rollback
            - jika ts == timestamp write Qk, maka overwrite
            - jika tidak, maka buat versi baru dengan timestamp write dan read = ts untuk Qk
        """
        
        latest_version = self._get_largest_ts(op, ts)
        
        if ts < self.data_items[op.opDataItem.name].versions[latest_version].ts_r:
            # rollback
            # print("ROLLLL BACK")
            log = f"ver: {op.opDataItem.name}{latest_version} TS({self.transactions[op.opTransaction].ts}) < R-ts({self.data_items[op.opDataItem.name].versions[latest_version].ts_r}) ROLLBACK"
            self.result_queue.append((op, ts, latest_version, log))
            return False
        
        elif ts == self.data_items[op.opDataItem.name].versions[latest_version].ts_w:
            # overwrite
            self.data_items[op.opDataItem.name].versions[latest_version].ts_w = ts
            self.data_items[op.opDataItem.name].versions[latest_version].ts_r = ts
            
            log = f"ver: {op.opDataItem.name}{latest_version} TS({self.transactions[op.opTransaction].ts}) == W-ts({op.opDataItem.name}{latest_version}) TS({op.opDataItem.name}{latest_version}) = ({self.data_items[op.opDataItem.name].versions[latest_version].ts_r}, {self.data_items[op.opDataItem.name].versions[latest_version].ts_w})"
            
            self.result_queue.append((op, ts, latest_version, log))
            # print("Write == ")
        else:
            # buat versi baru
            self.data_items[op.opDataItem.name].add_version(latest_version + 1)
            self.data_items[op.opDataItem.name].versions[latest_version + 1].ts_w = ts
            self.data_items[op.opDataItem.name].versions[latest_version + 1].ts_r = ts
            
            log = f"ver: {op.opDataItem.name}{latest_version + 1} NEW VERSION     TS({op.opDataItem.name}{latest_version + 1}) = ({self.data_items[op.opDataItem.name].versions[latest_version + 1].ts_r}, {self.data_items[op.opDataItem.name].versions[latest_version + 1].ts_w})"
            
            self.result_queue.append((op, ts, latest_version + 1, log))
            # print("Write else ")
            
            
        return True
            
            
    
        
    def _process(self, ope: Operation, ts: int) -> None:
        """ 
        Let data item, Qk, denote the version of Q whose write timestamp is the largest write timestamp less than or equal to TS(Ti)
        """
        success: bool = False
        match ope.opType:
            case "R":
                self._process_read(ope, ts)
            case "W":
                self._process_write(ope, ts)
    
    
    def run(self) -> None :
        while(not (self._is_ope_queue_empty() and self._is_aborted_queue_empty())):
            # proses abortqueue
            while(not self._is_aborted_queue_empty()):
                ope = self.aborted_queue.pop(0)
                ts = self.transactions[ope.opTransaction].ts
                self._process(ope, ts)
                
            # proses opequeue
            while(not self._is_ope_queue_empty()):
                ope = self.ope_queue.pop(0)
                ts = self.transactions[ope.opTransaction].ts
                self._process(ope, ts)