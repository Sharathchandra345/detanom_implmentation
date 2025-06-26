import sqlite3
import json
from itertools import product
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA = {
    'PERSONALINFO': {
        'table_id': 100,
        'attributes': {
            'EMPLOYEE_ID': 1,
            'SALARY': 2
        }
    },
    'WORKINFO': {
        'table_id': 200,
        'attributes': {
            'EMPLOYEE_ID': 1,
            'WORK_EXPERIENCE': 2
        }
    }
}

COMMAND_IDS = {
    'SELECT': 1,
    'INSERT': 2,
    'UPDATE': 3,
    'DELETE': 4  
}

def setup_database(row_count=50):
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE PersonalInfo (employee_id INTEGER, salary REAL)''')
    cursor.execute('''CREATE TABLE WorkInfo (employee_id INTEGER, work_experience INTEGER)''')
    for i in range(row_count):
        cursor.execute('INSERT INTO PersonalInfo VALUES (?, ?)', (i, 50000 + i * 100))
        cursor.execute('INSERT INTO WorkInfo VALUES (?, ?)', (i, 5 + (i % 20)))
    conn.commit()
    logging.info(f"Database initialized with {row_count} rows")
    return conn

def normalize_query(query):
    return ' '.join(query.strip().upper().replace(';', '').split())


def generate_signature(query):
    normalized_query = normalize_query(query)
    logging.info(f"Building signature for: {normalized_query}")
    parts = normalized_query.replace(',', ' ').split()
    command = parts[0]
    
    if command not in COMMAND_IDS:
        logging.error(f"Invalid command: {command}")
        return None
    
    command_id = COMMAND_IDS[command]
    signature = [command_id]
    
    if command == 'SELECT':
        try:
            attr_start = 1
            attr_end = parts.index('FROM')
            attrs = [attr for attr in parts[attr_start:attr_end] if attr not in (',')]
            table = parts[attr_end + 1]
            logging.info(f"SELECT attrs: {attrs}, table: {table}")
            
            if table not in SCHEMA:
                logging.error(f"Table {table} not in schema")
                return None
            table_id = SCHEMA[table]['table_id']
            attr_ids = []
            for attr in attrs:
                if attr in SCHEMA[table]['attributes']:
                    attr_ids.append((table_id, SCHEMA[table]['attributes'][attr]))
                else:
                    logging.error(f"Attribute {attr} not in schema for {table}")
                    return None
            signature.extend(attr_ids)
            signature.append((table_id,))
            
            where_attrs = []
            num_predicates = 0
            if 'WHERE' in normalized_query:
                where_clause = normalized_query.split('WHERE')[1].strip()
                predicates = where_clause.split('AND')
                num_predicates = len(predicates)
                for pred in predicates:
                    attr = pred.strip().split()[0]
                    if attr in SCHEMA[table]['attributes']:
                        where_attrs.append((table_id, SCHEMA[table]['attributes'][attr]))
                    else:
                        logging.error(f"WHERE attribute {attr} not in schema for {table}")
                        return None
            signature.append(tuple(where_attrs) if where_attrs else ())
            signature.append(num_predicates)
            logging.info(f"Signature: {signature}")
            
        except Exception as e:
            logging.error(f"Error parsing SELECT query: {e}")
            return None
            
    elif command == 'UPDATE':
        try:
            table = parts[1]
            if table not in SCHEMA:
                logging.error(f"Table {table} not in schema")
                return None
            table_id = SCHEMA[table]['table_id']
            
            set_clause = normalized_query.split('SET')[1].split('WHERE')[0].strip()
            set_attrs = [pair.split('=')[0].strip() for pair in set_clause.split(',')]
            set_attr_ids = []
            for attr in set_attrs:
                if attr in SCHEMA[table]['attributes']:
                    set_attr_ids.append((table_id, SCHEMA[table]['attributes'][attr]))
                else:
                    logging.error(f"SET attribute {attr} not in schema for {table}")
                    return None
            signature.extend(set_attr_ids)
            signature.append((table_id,))
            
            where_attrs = []
            num_predicates = 0
            if 'WHERE' in normalized_query:
                where_clause = normalized_query.split('WHERE')[1].strip()
                predicates = where_clause.split('AND')
                num_predicates = len(predicates)
                for pred in predicates:
                    attr = pred.strip().split()[0]
                    if attr in SCHEMA[table]['attributes']:
                        where_attrs.append((table_id, SCHEMA[table]['attributes'][attr]))
                    else:
                        logging.error(f"WHERE attribute {attr} not in schema for {table}")
                        return None
            signature.append(tuple(where_attrs))
            signature.append(num_predicates)
            logging.info(f"Signature: {signature}")
            
        except Exception as e:
            logging.error(f"Error parsing UPDATE query: {e}")
            return None
    
    return tuple(signature)


def extract_constraints(path_conditions):
    return path_conditions

#program
def salary_adjustment(profit, investment, conn, path_conditions, queries):
    cursor = conn.cursor()
    logging.info(f"Executing salary_adjustment with profit={profit}, investment={investment}")
    if profit >= 0.5 * investment:
        path_conditions.append(f"profit >= 0.5 * investment")
        query1 = "SELECT employee_id, salary FROM PersonalInfo WHERE salary > 50000"
        queries.append((query1, path_conditions[:]))
        cursor.execute(query1)
        resultset1 = cursor.fetchall()
        logging.info(f"Query 1 resultset size: {len(resultset1)}")
        if len(resultset1) > 100:
            path_conditions.append(f"resultset1_rows > 100")
            query3 = "UPDATE PersonalInfo SET salary = salary * 1.1 WHERE salary > 60000"
            queries.append((query3, path_conditions[:]))
            cursor.execute(query3)
        else:
            path_conditions.append(f"resultset1_rows <= 100")
            query2 = "UPDATE PersonalInfo SET salary = salary * 1.05 WHERE salary > 55000"
            queries.append((query2, path_conditions[:]))
            cursor.execute(query2)
    else:
        path_conditions.append(f"profit < 0.5 * investment")
        query4 = "SELECT employee_id, work_experience FROM WorkInfo WHERE work_experience > 10"
        queries.append((query4, path_conditions[:]))
        cursor.execute(query4)
    conn.commit()

# Profile builder 
def build_profile():
    profile = []

    input_ranges = [
        (50000, 100000, 150000), 
        (100000, 200000, 300000)  
    ]
    
    for row_count in [50, 150]:  
        conn = setup_database(row_count)
        for profit, investment in product(input_ranges[0], input_ranges[1]):
            path_conditions = []
            queries = []
            salary_adjustment(profit, investment, conn, path_conditions, queries)
            
            for query, conditions in queries:
                signature = generate_signature(query)
                if signature:
                    constraints = extract_constraints(conditions)
                    profile.append({
                        "query": query,
                        "signature": signature,
                        "constraints": constraints
                    })
        conn.close()
    
    unique_profile = []
    seen_queries = set()
    for entry in profile:
        if entry["query"] not in seen_queries:
            seen_queries.add(entry["query"])
            unique_profile.append(entry)
    
    with open('application_profile.json', 'w') as f:
        json.dump(unique_profile, f, indent=2, default=lambda x: list(x) if isinstance(x, tuple) else x)
    
    logging.info(f"Profile created with {len(unique_profile)} entries")
    return unique_profile

if __name__ == "__main__":
    build_profile()