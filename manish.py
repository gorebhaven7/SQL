def parse_conditions(tokens):
    conditions = []
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        if token == '(':
            # Find the corresponding closing parenthesis
            count = 1
            start_idx = idx + 1
            while count != 0 and idx < len(tokens) - 1:
                idx += 1
                if tokens[idx] == '(':
                    count += 1
                elif tokens[idx] == ')':
                    count -= 1
            sub_tokens = tokens[start_idx:idx]
            conditions.append(parse_conditions(sub_tokens))
            idx += 1  # Skip the closing parenthesis
        elif token in ['AND', 'OR']:
            conditions.append(token)
            idx += 1
        else:
            conditions.append(
                (token, tokens[idx + 1], tokens[idx + 2].strip(',')))
            idx += 3
    return conditions

def parse_query(query):
    tokens = query.split()

    fields = []
    conditions = []
    order_by = None

    if 'GET' in tokens:
        idx = tokens.index('GET')
        idx += 1
        while tokens[idx] not in ['WHERE', 'ORDER'] and idx < len(tokens):
            fields.append(tokens[idx].strip(','))
            idx += 1

    if 'WHERE' in tokens:
        idx = tokens.index('WHERE') + 1
        where_tokens = []
        while idx < len(tokens) and tokens[idx] != 'ORDER':
            where_tokens.append(tokens[idx])
            idx += 1
        conditions = parse_conditions(where_tokens)

    if 'ORDER' in tokens:
        idx = tokens.index('ORDER') + 2
        order_by = tokens[idx]

    return fields, conditions, order_by

def evaluate_condition(item, condition):
    field, operator, value = condition
    field = field.strip('()')
    value = value.strip('()')
    # print(field, operator, value)
    if operator == '>':
        return item[field] > int(value)
    elif operator == '<':
        return item[field] < int(value)
    elif operator == '=':
        return str(item[field]) == value
    else:
        raise ValueError(f"Unsupported operator: {operator}")

def evaluate_conditions(item, conditions):
    if not conditions:
        return True

    def eval_logical_ops(operand1, operator, operand2):
        if operator == 'AND':
            return operand1 and operand2
        elif operator == 'OR':
            return operand1 or operand2

    # Recursive function to evaluate nested conditions
    def recursive_eval(conds):
        if not conds:
            return True

        if len(conds) == 1:
            return conds[0] if isinstance(conds[0], bool) else evaluate_condition(item, conds[0])

        if 'AND' in conds or 'OR' in conds:
            for idx, val in enumerate(conds):
                if val in ['AND', 'OR']:
                    left_result = recursive_eval(conds[:idx])
                    right_result = recursive_eval(conds[idx + 1:])
                    return eval_logical_ops(left_result, val, right_result)

        return evaluate_condition(item, conds)

    return recursive_eval(conditions)


    # Evaluate the result stack
    while len(result_stack) > 1:
        operand1 = result_stack.pop(0)
        operator = result_stack.pop(0)
        operand2 = result_stack.pop(0)

        if operator == 'AND':
            result = operand1 and operand2
        elif operator == 'OR':
            result = operand1 or operand2

        result_stack.insert(0, result)

    return result_stack[0] if result_stack else True
    return result


def execute_query(data, query):
    fields, conditions, order_by = parse_query(query)

    # Filtering
    filtered_data = []
    for item in data:
        if evaluate_conditions(item, conditions):
            filtered_data.append(item)

    data = filtered_data

    # Ordering
    if order_by:
        data = sorted(data, key=lambda x: x[order_by])

    # Selecting fields
    results = []
    for item in data:
        result_item = {field: item.get(field, None) for field in fields}
        results.append(result_item)

    return results


def read_data_in_chunks(data_source, chunk_size):
    """Generator to yield chunks of data."""
    chunk = []
    for item in data_source:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def execute_query(data_source, query, chunk_size=10):
    fields, conditions, order_by = parse_query(query)

    # Process data in chunks
    for chunk in read_data_in_chunks(data_source, chunk_size):
        # Filtering
        filtered_chunk = [item for item in chunk if evaluate_conditions(item, conditions)]

        # Ordering
        if order_by:
            filtered_chunk.sort(key=lambda x: x[order_by])

        # Selecting fields
        for item in filtered_chunk:
            result_item = {field: item.get(field, None) for field in fields}
            yield result_item

# Example data and query
data = [
    {"name": "John", "age": 25, "city": "NewYork"},
    {"name": "Jane", "age": 30, "city": "LosAngeles"},
    {"name": "Doe", "age": 28, "city": "SanFrancisco"}
]
query = "GET name, age WHERE (name = Jane AND age > 25) OR (city = SanFrancisco OR city = NewYork)"

for result in execute_query(data, query, chunk_size=2):
    print(result)

# query = "GET name, age WHERE name = John"
# print(execute_query(data, query))
# query = "GET name, age WHERE name = John OR age > 25"
# print(execute_query(data, query))
# query = "GET name, age WHERE name = John OR age > 25 ORDER BY age"
# print(execute_query(data, query))
# query = "GET name, age WHERE (name = Jane AND age > 25) OR (name = Doe OR city = NewYork) "
# print(execute_query(data, query))
# query = "GET name, age WHERE (name = Jane AND age > 25) AND (name = Doe OR city = NewYork) "
# print(execute_query(data, query))
# query = "GET name, age WHERE (name = Jane AND age > 25) AND (city = SanFrancisco OR city = NewYork) "
# print(execute_query(data, query))