import csv
import os
import json
import re

def process_command(command):
    cmd_parts = command.split(maxsplit=2)
    print(cmd_parts)
    cmd_type = cmd_parts[0]

    if cmd_type == "create_table":
        return create_table_command(cmd_parts)

    elif cmd_type == "insert_into":
        return insert_into_command(cmd_parts)
    elif cmd_type == "select":
        return select_command(cmd_parts)
    else:
        return "Unknown command"

def create_table_command(cmd_parts):
    if len(cmd_parts) < 3:
        return "Invalid create_table command format"
    filename, columns = cmd_parts[1], cmd_parts[2]
    columns = columns.split()
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
    if len(cmd_parts) != 3:
        return "Invalid insert_into command format"
    filename, column_data = cmd_parts[1], cmd_parts[2]
    if not os.path.exists(filename):
        return f"Table {filename} does not exist."

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
        update_meta_file(filename, increment=True)

    return f"Values inserted into {filename}."

def select_command(cmd_parts):
    if len(cmd_parts) < 3:
        return "Invalid select command format"
    
    command_str = ' '.join(cmd_parts[1:])
    try:
        columns_part, rest = command_str.split(' from ')
        columns = [col.strip() for col in columns_part.split(',')]
        filename, where_clause = rest.split(' where ')
        # and_operator = ' AND ' in where_clause
        # conditions = parse_where_clause(where_clause)
        conditions, logical_operator = parse_where_clause(where_clause)
    except ValueError:
        return "Invalid select command format. Ensure you use 'select col1, col2 from filename where condition'."

    filename = filename.strip()
    if not filename.endswith('.csv'):
        filename += '.csv'

    if not os.path.exists(filename):
        return f"Table {filename} does not exist."

    meta_file = 'meta.json'
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as file:
            meta_data = json.load(file)
            table_lines = meta_data.get(filename, 0)
            chunk_line_count = max(1, table_lines // 5)
    else:
        chunk_line_count = 1  # Default value if meta file doesn't exist
    print("Chunk line count:", chunk_line_count)
    process_file_in_chunks(filename, columns, chunk_line_count, conditions, logical_operator)

    return f"Select query executed on {filename}."


def process_file_in_chunks(filename, columns, chunk_line_count, conditions, logical_operator):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        selected_columns = [col for col in columns if col in reader.fieldnames]
        print(','.join(selected_columns))  # Print column headers

        chunk_count, chunk = 0, []
        for row in reader:
            chunk.append(row)

        if len(chunk) >= chunk_line_count:
            filtered_chunk = [r for r in chunk if not conditions or check_condition(r, conditions, logical_operator)]
            process_chunk(filtered_chunk, chunk_count, selected_columns)
            chunk, chunk_count = [], chunk_count + 1

        if chunk:  # Process last chunk
            and_operator = ' AND ' in conditions
            filtered_chunk = [r for r in chunk if not conditions or check_condition(r, conditions, and_operator)]
            process_chunk(filtered_chunk, chunk_count, selected_columns)

def process_chunk(chunk, chunk_count, selected_columns):
    # print(f"Chunk {chunk_count}:")
    for row in chunk:
        print(','.join(row[col] for col in selected_columns))


def check_condition(row, conditions, logical_operator):
    results = []

    for column_name, operator, value in conditions:
        try:
            row_value = row[column_name]
            # Check each condition
            result = False
            if operator == '==':
                result = row_value == value
            elif operator in ['>', '<', '>=', '<=', '!=']:
                result = eval(f'{float(row_value)} {operator} {float(value)}')
            results.append(result)
        except KeyError:
            results.append(False)
    if logical_operator == 'AND':
        return all(results)
    elif logical_operator == 'OR':
        return any(results)
    else:
        return results[0] if results else False

    # Apply AND or OR logic to the results
    # return all(results) if and_operator else any(results)

def update_meta_file(table_name, increment=False):
    meta_file = 'meta.json'
    meta_data = {}

    # Read existing meta data
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as file:
            meta_data = json.load(file)

    # Update or set the line count
    if table_name in meta_data:
        if increment:
            meta_data[table_name] += 1
    else:
        meta_data[table_name] = 1 if increment else 0

    # Write back to the meta file
    with open(meta_file, 'w') as file:
        json.dump(meta_data, file)


def parse_where_clause(where_clause):
    if ' AND ' in where_clause:
        operator = 'AND'
        conditions = where_clause.split(' AND ')
    elif ' OR ' in where_clause:
        operator = 'OR'
        conditions = where_clause.split(' OR ')
    else:
        operator = None
        conditions = [where_clause]

    parsed_conditions = []
    for condition in conditions:
        parts = condition.split()
        if len(parts) != 3:
            continue
        column_name, condition_operator, value = parts
        parsed_conditions.append((column_name, condition_operator, value))

    return parsed_conditions, operator

# Command-Line Interface
while True:
    command = input("MyDB > ")
    if command == "exit":
        break
    response = process_command(command)
    print(response)
