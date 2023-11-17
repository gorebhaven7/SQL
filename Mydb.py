import csv
import os
import json

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
        condition = parse_where_clause(where_clause)
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
    process_file_in_chunks(filename, columns, chunk_line_count, condition)

    return f"Select query executed on {filename}."

def process_file_in_chunks(filename, columns, chunk_line_count, condition):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        header = reader.fieldnames
        selected_columns = [col for col in columns if col in header]
        print(','.join(selected_columns))  # Print the column headers once

        chunk_count = 0
        chunk = []
        for row in reader:
            chunk.append({col: row[col] for col in selected_columns})

            if len(chunk) >= chunk_line_count:
                # Apply condition to the chunk and process it
                filtered_chunk = [r for r in chunk if not condition or check_condition(r, condition)]
                process_chunk(filtered_chunk, chunk_count, selected_columns)
                chunk = []  # Reset the chunk
                chunk_count += 1

        # Process the last chunk if it's not empty
        if chunk:
            filtered_chunk = [r for r in chunk if not condition or check_condition(r, condition)]
            process_chunk(filtered_chunk, chunk_count, selected_columns)

def process_chunk(chunk, chunk_count, selected_columns):
    print(f"Chunk {chunk_count}:")
    for row in chunk:
        print(','.join(row[col] for col in selected_columns))

def check_condition(row, condition):
    column_name, operator, value = condition
    try:
        row_value = row[column_name]
        # Add logic to handle different operators
        if operator == '==':
            return row_value == value
        elif operator == '>':
            return float(row_value) > float(value)
        # Add more operators as needed
    except KeyError:
        return False

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
    parts = where_clause.split()
    if len(parts) != 3:
        return None
    column_name, operator, value = parts
    return (column_name, operator, value)

# Command-Line Interface
while True:
    command = input("MyDB > ")
    if command == "exit":
        break
    response = process_command(command)
    print(response)
