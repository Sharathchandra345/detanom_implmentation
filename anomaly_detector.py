import sqlite3
import json
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

def setup_database():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE PersonalInfo (employee_id INTEGER, salary REAL)''')
    cursor.execute('''CREATE TABLE WorkInfo (employee_id INTEGER, work_experience INTEGER)''')
    for i in range(50):
        cursor.execute('INSERT INTO PersonalInfo VALUES (?, ?)', (i, 50000 + i * 100))
        cursor.execute('INSERT INTO WorkInfo VALUES (?, ?)', (i, 5 + (i % 20)))
    conn.commit()
    logging.info("Database initialized with 50 rows")
    return conn

def normalize_query(query):
    return ' '.join(query.strip().upper().replace(';', '').split())

def generate_signature(query):
    normalized_query = normalize_query(query)
    logging.info(f"Generating signature for: {normalized_query}")
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
            signature.append(tuple(where_attrs) if where_attrs else ())
            signature.append(num_predicates)
            logging.info(f"Signature: {signature}")
            
        except Exception as e:
            logging.error(f"Error parsing UPDATE query: {e}")
            return None
    
    return tuple(signature)

def list_to_tuple(item):
    if isinstance(item, list):
        return tuple(list_to_tuple(x) for x in item)
    return item

# Anomaly Detection
class AnomalyDetectionEngine:
    def __init__(self, profile_path='application_profile.json'):
        self.profile = self.load_profile(profile_path)
        self.strict_policy = len(self.profile) >= 4
        self.flagged_queries = []
        logging.info(f"Loaded profile with {len(self.profile)} entries. Using {'strict' if self.strict_policy else 'flexible'} policy.")

    def load_profile(self, profile_path):
        try:
            with open(profile_path, 'r') as f:
                profile = json.load(f)
            for entry in profile:
                entry['signature'] = list_to_tuple(entry['signature'])
                logging.info(f"Loaded signature for {entry['query']}: {entry['signature']}")
            return profile
        except FileNotFoundError:
            logging.error("Profile file not found")
            return []

    def check_query(self, query, current_constraints):
        signature = generate_signature(query)
        if not signature:
            logging.warning(f"Invalid query signature: {query}")
            return self.handle_anomaly(query, current_constraints)

        for entry in self.profile:
            if entry['signature'] == signature:
                profile_constraints = set(entry['constraints'])
                current_constraints_set = set(current_constraints)
                if profile_constraints.issubset(current_constraints_set):
                    logging.info(f"Query passed: {query}")
                    return True
                else:
                    logging.warning(f"Constraints mismatch for {query}. Profile: {profile_constraints}, Current: {current_constraints_set}")
                    return self.handle_anomaly(query, current_constraints)

        logging.warning(f"No matching signature for query: {query}")
        return self.handle_anomaly(query, current_constraints)

    def handle_anomaly(self, query, current_constraints):
        if self.strict_policy:
            logging.error(f"Anomaly detected (strict policy): {query}")
            return False
        else:
            self.flagged_queries.append((query, current_constraints))
            logging.warning(f"Query flagged (flexible policy): {query}")
            return True

# Query Interceptor
class QueryInterceptor:
    def __init__(self, conn, ade):
        self.conn = conn
        self.ade = ade
        self.cursor = conn.cursor()

    def execute_query(self, query, current_constraints):
        if self.ade.check_query(query, current_constraints):
            try:
                self.cursor.execute(query)
                if query.strip().upper().startswith('SELECT'):
                    return self.cursor.fetchall()
                self.conn.commit()
                return None
            except Exception as e:
                logging.error(f"Query execution failed: {e}")
                return None
        else:
            logging.error(f"Query blocked: {query}")
            return None

# SalaryAdjustment program
def salary_adjustment(profit, investment, interceptor, path_conditions):
    results = []
    logging.info(f"Testing salary_adjustment with profit={profit}, investment={investment}")
    if profit >= 0.5 * investment:
        path_conditions.append(f"profit >= 0.5 * investment")
        query1 = "SELECT employee_id, salary FROM PersonalInfo WHERE salary > 50000"
        resultset1 = interceptor.execute_query(query1, path_conditions[:])
        if resultset1:
            results.append(resultset1)
            logging.info(f"Query 1 resultset size: {len(resultset1)}")
            if len(resultset1) > 100:
                path_conditions.append(f"resultset1_rows > 100")
                query3 = "UPDATE PersonalInfo SET salary = salary * 1.1 WHERE salary > 60000"
                interceptor.execute_query(query3, path_conditions[:])
            else:
                path_conditions.append(f"resultset1_rows <= 100")
                query2 = "UPDATE PersonalInfo SET salary = salary * 1.05 WHERE salary > 55000"
                interceptor.execute_query(query2, path_conditions[:])
    else:
        path_conditions.append(f"profit < 0.5 * investment")
        query4 = "SELECT employee_id, work_experience FROM WorkInfo WHERE work_experience > 10"
        resultset4 = interceptor.execute_query(query4, path_conditions[:])
        if resultset4:
            results.append(resultset4)
    return results

def main():
    conn = setup_database()
    ade = AnomalyDetectionEngine()
    interceptor = QueryInterceptor(conn, ade)
    
    logging.info("Testing legitimate execution...")
    path_conditions = []
    salary_adjustment(150000, 200000, interceptor, path_conditions)
    
    logging.info("Testing malicious query...")
    malicious_query = "SELECT employee_id FROM PersonalInfo WHERE salary < 1000"
    interceptor.execute_query(malicious_query, ["profit >= 0.5 * investment"])
    
    conn.close()

if __name__ == "__main__":
    main()