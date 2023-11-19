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
        rpn_expression = parse_where_clause(where_clause)
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
    process_file_in_chunks(filename, columns, chunk_line_count, rpn_expression)

    return f"Select query executed on {filename}."


def process_file_in_chunks(filename, columns, chunk_line_count, rpn_expression):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        selected_columns = [col for col in columns if col in reader.fieldnames]
        print(','.join(selected_columns))  # Print column headers

        chunk_count, chunk = 0, []
        for row in reader:
            chunk.append(row)

            if len(chunk) >= chunk_line_count:
                filtered_chunk = [r for r in chunk if not rpn_expression or evaluate_rpn_expression(rpn_expression, r)]
                process_chunk(filtered_chunk, chunk_count, selected_columns)
                chunk, chunk_count = [], chunk_count + 1

        if chunk:  # Process last chunk
            filtered_chunk = [r for r in chunk if not rpn_expression or evaluate_rpn_expression(rpn_expression, r)]
            process_chunk(filtered_chunk, chunk_count, selected_columns)

def process_chunk(chunk, chunk_count, selected_columns):
    # print(f"Chunk {chunk_count}:")
    for row in chunk:
        print(','.join(row[col] for col in selected_columns))


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
    tokens = tokenize_where_clause(where_clause)
    rpn_expression = shunting_yard(tokens)
    # rpn_expression =['departement', 'finance', '==',]
    return rpn_expression

def tokenize_where_clause(where_clause):
    # Updated pattern to correctly split around logical operators, numbers, and words
    pattern = r'(\band\b|\bor\b|\(|\)|\!=|==|<=|>=|<|>|\d+|\b\w+\b|"[^"]*"|\'[^\']*\')'
    tokens = re.split(pattern, where_clause)
    return [token.strip() for token in tokens if token.strip()]


def shunting_yard(tokens):
    precedence = {'OR': 1, 'AND': 2, '==': 3, '!=': 3, '<': 3, '>': 3, '<=': 3, '>=': 3}
    output_queue = []
    operator_stack = []

    for token in tokens:
        print(f"Processing token: {token}")
        if token.isidentifier() or token.replace('.', '', 1).isdigit() or token[0] in ['"', "'"]:
            output_queue.append(token)
        elif token in precedence:
            while operator_stack and operator_stack[-1] != '(' and precedence[operator_stack[-1]] >= precedence[token]:
                output_queue.append(operator_stack.pop())
            operator_stack.append(token)
        elif token == '(':
            operator_stack.append(token)
        elif token == ')':
            while operator_stack and operator_stack[-1] != '(':
                output_queue.append(operator_stack.pop())
            operator_stack.pop()  # Remove the '('

    while operator_stack:
        output_queue.append(operator_stack.pop())

    print(f"Final RPN: {output_queue}")  # Debug print at the end
    return output_queue

def evaluate_rpn_expression(rpn_expression, row):
    stack = []
    operators = {'OR', 'AND', '==', '!=', '<', '>', '<=', '>='}
    print(f"Evaluating RPN: {rpn_expression}")

    for token in rpn_expression:
        if token not in operators:  # Check if the token is not an operator
            if token.replace('.', '', 1).isdigit() or token[0] in ['"', "'"]:
                stack.append(token.strip('"\''))  # Handle literals
            else:
                stack.append(row.get(token, ''))  # Assume it's a column name
        else:
            operand2 = stack.pop()
            operand1 = stack.pop()
            result = evaluate_condition(operand1, token, operand2)
            print(f"{operand1} {token} {operand2} = {result}")
            stack.append(result)

    return stack[0] if stack else None


def evaluate_condition(operand1, operator, operand2):
    operand1 = convert_operand(operand1)
    operand2 = convert_operand(operand2)

    # Ensure comparison is between compatible types
    if isinstance(operand1, type(operand2)) or isinstance(operand2, type(operand1)):
        if operator == '==':
            return operand1 == operand2
        elif operator == '!=':
            return operand1 != operand2
        elif operator in ['>', '<', '>=', '<=']:
            # For numeric comparisons, both operands should be numbers
            if isinstance(operand1, (int, float)) and isinstance(operand2, (int, float)):
                if operator == '>':
                    return operand1 > operand2
                elif operator == '<':
                    return operand1 < operand2
                elif operator == '>=':
                    return operand1 >= operand2
                elif operator == '<=':
                    return operand1 <= operand2
        elif operator == 'AND':
            return bool(operand1) and bool(operand2)
        elif operator == 'OR':
            return bool(operand1) or bool(operand2)
    return False  # In case of incompatible types or unknown operator


def convert_operand(operand):
    if isinstance(operand, str):
        try:
            return float(operand) if operand.replace('.', '', 1).isdigit() else operand
        except ValueError:
            return operand
    return operand 

# Command-Line Interface
while True:
    command = input("MyDB > ")
    if command == "exit":
        break
    response = process_command(command)
    print(response)
