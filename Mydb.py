import re
import os
import csv
import json
from collections import defaultdict

def process_command(command):
    cmd_parts = command.split(maxsplit=2)
    
    cmd_type = cmd_parts[0]          

    if cmd_type == "create_table":
        return create_table_command(cmd_parts)

    elif cmd_type == "insert_into":
        return insert_into_command(cmd_parts)

    elif cmd_type == "select":
        return select_command(cmd_parts)
    
    elif cmd_type == "delete":
        return delete_command(cmd_parts)

    else:
        return "Unknown command"

def create_table_command(cmd_parts):
    #Eg: create_table student id,name,age
    if len(cmd_parts) < 3:
        return "Invalid create_table command format"

    filename, columns = cmd_parts[1], cmd_parts[2]
    columns = columns.split(",")
    if not filename.endswith('.csv'):
        filename += '.csv'

    # Check if the file already exists
    if os.path.exists(filename):
        return f"Table {filename} already exists."

    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()

    return f"Table {filename} created with columns {', '.join(columns)}."

def insert_into_command(cmd_parts):
    #Eg: insert_into student id=1,name=Bhaven,age=25
    #Eg: insert_into student id=2,name=Prads,age=24
    if len(cmd_parts) != 3:
        return "Invalid insert_into command format"

    filename, column_data = cmd_parts[1], cmd_parts[2]
    if not filename.endswith('.csv'):
        filename += '.csv'

    # Parsing column data
    data = {}
    for pair in column_data.split(','):
        key, value = pair.split('=')
        data[key.strip()] = value.strip().strip('"')

    # Read the header to maintain the column order
    with open(filename, 'r', newline='') as file:
        reader = csv.reader(file)
        header = next(reader)

    # Map the data to the column order in the header
    ordered_data = {column: data.get(column, '') for column in header}

    # Write the ordered data
    with open(filename, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writerow(ordered_data)

    return f"Values inserted into {filename}."

def select_command(cmd_parts):
    #Eg: select id,name from student where id==2
    #Eg: select id,name from employees where id>2 AND department==Finance
    #Eg: select department,COUNT() from employees group_by department
    # print(cmd_parts)
    if len(cmd_parts) < 2:
        return "Invalid select command format"
    
    command_str = ' '.join(cmd_parts[1:])
    # try:
    columns_part, rest = command_str.split(' from ')
    columns = [col.strip() for col in columns_part.split(',')]

    if "group_by" in rest:
        filename, group_by = rest.split(' group_by ')
        if not filename.endswith('.csv'):
            filename += '.csv'

        aggregate_info = columns[1].split('(')  # Splitting to get aggregate function and column
        aggregate_func = aggregate_info[0].upper()  # Getting the aggregate function (SUM, MAX, MIN, COUNT)
        aggregate_col = aggregate_info[1][:-1] if aggregate_func != "COUNT" else None  # Getting the column for aggregation

        chunk_line_count = get_number_lines(filename)
        perform_groupBy(filename, columns[0], aggregate_func, aggregate_col, chunk_line_count)

    elif "join" in rest:
        table1,on_part = rest.split(' join ')
        table2,condition = on_part.split(' on ')
        if 'where' in condition:
            join_condtition,where_clause = condition.split(' where ')
            perform_join(columns,table1,table2,join_condtition,columns,where_clause)
        else:
            perform_join(columns,table1,table2,condition,columns)

    elif "where" in rest:
        filename, where_clause = rest.split(' where ')
        # print("Where",where_clause)
        conditions,order_by = parse_conditions(where_clause)
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        chunk_line_count = get_number_lines(filename)
        execute_query(filename, columns, conditions, order_by, chunk_line_count)

    else:
        filename = rest
        if not filename.endswith('.csv'):
            filename += '.csv'

        chunk_line_count = get_number_lines(filename)

        execute_query(filename, columns, None, None, chunk_line_count=10)

    # except ValueError:
    #     return "Invalid select command format: "

    return f"Select query executed."

def perform_groupBy(filename, group_field, aggregate_func, aggregate_field, chunk_line_count=10):
    #Eg: select department, COUNT() from employees group_by department
    #Eg: select depaartment, SUM(salary) from employees group_by department
    #Eg: select name, MAX(age) from student group_by age
    #Eg: select name, MIN(age) from student group_by age
    group_results = defaultdict(lambda: {"SUM": 0, "COUNT": 0, "MAX": float('-inf'), "MIN": float('inf')})

    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        chunk = []

        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_line_count:
                process_chunk2(chunk, group_results, group_field, aggregate_func, aggregate_field)
                chunk = []  # Reset the chunk

        if chunk:  # Process the last chunk if it exists
            process_chunk2(chunk, group_results, group_field, aggregate_func, aggregate_field)

    # print(group_results)
    # Print or process the group results as needed
    for group, values in group_results.items():
        result = values[aggregate_func]
        print(f"{group}: {aggregate_func} = {result}")

def process_chunk2(chunk, group_results, group_field, aggregate_func, aggregate_field):
    for row in chunk:
        group_value = row[group_field]

        try:
            aggregate_value = float(row[aggregate_field]) if aggregate_field and row[aggregate_field] else 0
        except ValueError:
            print(f"Warning: Invalid value for aggregation field '{aggregate_field}' in row: {row}")
            aggregate_value = 0

        if aggregate_func == "MAX":
            group_results[group_value]["MAX"] = max(group_results[group_value]["MAX"], aggregate_value)
        elif aggregate_func == "MIN":
            group_results[group_value]["MIN"] = min(group_results[group_value]["MIN"], aggregate_value)
        elif aggregate_func == "SUM":
            group_results[group_value]["SUM"] += aggregate_value
        elif aggregate_func == "COUNT":
            group_results[group_value]["COUNT"] += 1


def perform_join(columns,table1,table2,condition,fields,where_clause=None,chunk_line_count=10):
    #eg: select id,name from student.csv join employees.csv on student.id==employees.id
    #eg: select id,name from employees.csv join employees.csv on employees.id==employees.id
    #eg: select id,name from student join employees on student.id==employees.id where student.id>2
    
    condition = condition.split('==') 
    table1_key = condition[0].split('.')[-1]  
    table2_key = condition[1].split('.')[-1]  
    # print(condition,where_clause)
    join_file = table1.split('.')[0] + '_' + table2.split('.')[0] + '.csv'
    delete_file_if_exists(join_file)
    chunk_line_count1 = get_number_lines(table1)
    chunk_line_count2 = get_number_lines(table2)
    if where_clause!=None:
        where_condition = parse_conditions(where_clause)
    # print("where",where_condition)
    filename1 = table1
    if not table1.endswith('.csv'):
        filename1 += '.csv'
    filename2 = table2
    if not table2.endswith('.csv'):
        filename2 += '.csv'

    with open(filename1, 'r', newline='') as file1:
        reader1 = csv.DictReader(file1)
        for chunk1 in chunk_reader(reader1, chunk_line_count1):
            with open(filename2, 'r', newline='') as file2:
                reader2 = csv.DictReader(file2)
                for chunk2 in chunk_reader(reader2, chunk_line_count2):
                    for row1 in chunk1:
                        for row2 in chunk2:
                            if row1[table1_key] == row2[table2_key]:
                                joined_row = prefix_row_keys(row1, table1) | prefix_row_keys(row2, table2)
                                if where_clause!=None:
                                    filtered_row = evaluate_conditions(joined_row, where_condition[0])
                                    if filtered_row:
                                        print("Joined",joined_row)
                                        write_to_csv_json(joined_row, join_file)
                                else:
                                    print("Joined",joined_row)
                                    write_to_csv_json(joined_row, join_file)

def write_to_csv(chunk, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        for r in chunk:
            writer.writerow(r.values())

def write_to_csv_json(row, filename):
    with open(filename, 'a', newline='') as file:
        fieldnames = row.keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if file.tell() == 0: 
            writer.writeheader()

        writer.writerow(row)

def delete_file_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted existing file: {file_path}")

def get_prefixed_fieldnames(fieldnames, prefix):
    return [f"{prefix}.{fieldname}" for fieldname in fieldnames]

def prefix_row_keys(row, prefix):
    return {f"{prefix}.{k}": v for k, v in row.items()}

def chunk_reader(reader, chunk_size):
    chunk = []
    for row in reader:
        chunk.append(row)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def parse_conditions(where_clause):
    
    if 'ORDER_BY' in where_clause:
        where_clause, order_by = where_clause.split('ORDER_BY')
        order_by = order_by.split()
    else:
        order_by = None
    tokens = re.split(r'(\(|\)|AND|OR|==|>|<|!=|<=|>=)', where_clause)
    tokens = [token.strip() for token in tokens if token.strip()]

    tks = tokens.copy()
    tks_stack = []
    while tks:
        token = tks.pop()
        if token == ')':
            temp_stack = []
            while len(tks)>1 and tks[-1] != '(':
                temp_stack.append(tks.pop())
            tks_stack.append(temp_stack[::-1])
            tks.pop()
        else:
            tks_stack.append(token)
    return None if not tks_stack else tks_stack[::-1], order_by

def execute_query(filename,fields=None, conditions=None,ordersel_by=None, chunk_line_count=10):

    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        selected_columns = [col for col in fields if col in reader.fieldnames]

        chunk_count, chunk,result = 0, [],[]
        for row in reader:
            chunk.append(row)

            if len(chunk) >= chunk_line_count:
                filtered_chunk = [r for r in chunk if not conditions or evaluate_conditions(r, conditions)]
                result.extend(process_chunk(filtered_chunk, chunk_count, selected_columns))
                chunk, chunk_count = [], chunk_count + 1
                write_to_csv(filtered_chunk, "temp_"+str(chunk_count)+".csv")

        if chunk:  # Process last chunk
            filtered_chunk = [r for r in chunk if not conditions or evaluate_conditions(r, conditions)]
            result.extend(process_chunk(filtered_chunk, chunk_count, selected_columns))
            write_to_csv(filtered_chunk, "temp_"+str(chunk_count)+".csv")
    print(result)

def process_chunk(chunk, chunk_count, selected_columns):
    ans = []
    for row in chunk:
        ans.append(row)
    return ans

def evaluate_condition(item, condition):
    field, operator, value = condition
    field = field.strip('()')
    value = value.strip('()')
    if not item[field]:
        if operator == '==':
            item[field] = '0'
        else:
            item[field] = 0
    if operator == '>':
        return int(item[field]) > int(value)
    elif operator == '<':
        return int(item[field]) < int(value)
    elif operator == '>=':
        return int(item[field]) >= int(value)
    elif operator == '<=':
        return int(item[field]) <= int(value)
    elif operator == '==':
        return str(item[field]) == value
    else:
        raise ValueError(f"Unsupported operator: {operator}")

def evaluate_conditions(item, conditions):
    # print(item,conditions)
    if not conditions:
        return True

    def eval_logical_ops(operand1, operator, operand2):
        if operator == 'AND':
            return operand1 and operand2
        elif operator == 'OR':
            return operand1 or operand2

    def recursive_eval(conds):
        if not conds:
            return True
        
        if 'AND' in conds or 'OR' in conds:
            for idx, val in enumerate(conds):
                if val in ['AND', 'OR']:
                    left, right = conds[:idx], conds[idx+1:]
                    if type(left[0]) == list:
                        left_result = recursive_eval(left[0])
                    else:
                        left_result = recursive_eval(left)
                    if type(right[0]) == list:
                        right_result = recursive_eval(right[0])
                    else:
                        right_result = recursive_eval(right)
                    return eval_logical_ops(left_result, val, right_result)

        return evaluate_condition(item, conds)

    return recursive_eval(conditions)

def delete_command(cmd_parts):
    #Eg: delete from student where age==26
    if len(cmd_parts) < 2:
        return "Invalid select command format"
    
    command_str = ' '.join(cmd_parts[1:])
    # try:
    columns_part = command_str.split('from ')
    if "where" in columns_part[1]:
        filename, where_clause = columns_part[1].split(' where ')
        # print("Where",where_clause)
        conditions,order_by = parse_conditions(where_clause)
        print(conditions)
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        chunk_line_count = get_number_lines(filename)
        execute_query_delete(filename, conditions, chunk_line_count)


def execute_query_delete(filename, conditions=None, chunk_line_count=10):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        selected_columns = [col for col in reader.fieldnames]
        print(selected_columns)
        newfile = "new_file.csv"
        chunk_count, chunk,result = 0, [],[]
        for row in reader:
            chunk.append(row)

            if len(chunk) >= chunk_line_count:
                for r in chunk:
                    if not evaluate_conditions(r, conditions):
                        print("Deleted",r)
                        chunk, chunk_count = [], chunk_count + 1
                        write_to_csv_json(row, newfile)

        if chunk:  # Process last chunk
            for r in chunk:
                if not evaluate_conditions(r, conditions):
                    print("Deleted",r)
                    chunk, chunk_count = [], chunk_count + 1
                    write_to_csv_json(row, newfile)
        
    os.rename(newfile, filename)
    return None


def get_number_lines(filename):
    meta_file = 'meta.json'
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as file:
            meta_data = json.load(file)
            table_lines = meta_data.get(filename, 0)
            chunk_line_count = max(1, table_lines // 5)
    else:
        chunk_line_count = 1 
    print("Chunk line count:", chunk_line_count)
    return chunk_line_count

while True:
    command = input("MyDB > ")
    if command == "exit":
        break
    response = process_command(command)
    print(response)