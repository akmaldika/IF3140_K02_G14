from model.Operation import Operation
import shutil


# Define widths for each part of the output
op_type_width = 15 
data_item_name_width = 15  
transaction_id_width = 15  


def print_title(text):
    terminal_width, _ = shutil.get_terminal_size()
    line = "-" * terminal_width
    padding = (terminal_width - len(text)) // 2
    print(line)
    print(line[:padding] + text + line[padding + len(text):])
    print(line)


def print_process(op_type: str, transaction_id: str, data_item_name: str, descriptions: list[str]):
    op_type_mapping = {
        "R": "Read",
        "W": "Write",
        "C": "Commit",
        "A": "Abort",
        "XL": "Grant X-Lock",
        "SL": "Grant S-Lock",
        "UL": "Unlock",
        "TW": "Temp Write"
    }
    op_type = op_type_mapping.get(op_type, op_type)

    description_str = ' | '.join(descriptions)

    # Format the output with specified widths
    output = f"{op_type.ljust(op_type_width)}{data_item_name.ljust(data_item_name_width)}T{transaction_id.ljust(transaction_id_width)}{description_str}"

    print(output)

    
def print_header():
    # Format the header with specified widths
    header = f"{'Operation'.ljust(op_type_width)}{'Data Item'.ljust(data_item_name_width)}{'Transaction'.ljust(transaction_id_width)} Description"
    print()
    print(header)


def print_result(result: list[Operation]):
    print("Result: ")
    for i in result:
      print(i.opName + "; ", end="")
