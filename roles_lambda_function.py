import json
import boto3
import psycopg2
import oracledb  

# Retrieve secrets from AWS Secrets Manager
def get_secret(secret_name):
    try:
        client = boto3.client('secretsmanager')
        secret = client.get_secret_value(SecretId=secret_name)
        return json.loads(secret['SecretString'])
    except Exception as e:
        print(f"ERROR retrieving secret {secret_name}: {e}")
        return None

# Extract MongoDB _id
def extract_id(doc):
    _id_raw = doc.get('_id')
    if isinstance(_id_raw, dict) and '$oid' in _id_raw:
        return _id_raw['$oid']
    return str(_id_raw) if _id_raw else None

# Redshift operations
def insert_redshift_role(cursor, role):
    id = extract_id(role)
    if not id:
        print("Missing _id for insert.")
        return
    data = json.dumps(role)
    last_update_date = role.get('last_update_date', None)
    cursor.execute("INSERT INTO documentdb.roles (id, data, last_update_date) VALUES (%s, %s, %s);", (id, data, last_update_date))
    print(f"Inserted Redshift role {id}")

def update_redshift_role(cursor, role):
    id = extract_id(role)
    if not id:
        print("Missing _id for update.")
        return
    data = json.dumps(role)
    last_update_date = role.get('last_update_date', None)
    cursor.execute("UPDATE documentdb.roles SET data = %s, last_update_date = %s WHERE id = %s;", (data, last_update_date, id))
    if cursor.rowcount == 0:
        insert_redshift_role(cursor, role)
    else:
        print(f"Updated Redshift role {id}")

# Oracle operations
def update_oracle_role(cursor, role, operation_type):
    role_id = role.get('role_id')
    if not role_id:
        print("Missing role_id for Oracle operation.")
        return

    is_deleted = role.get('is_deleted', False)

    if operation_type in ['insert', 'update', 'replace'] and not is_deleted:
        params = {
            'role_id': role_id,
            'role_name': role.get('role_name'),
            'role_description': role.get('role_description'),
            'role_status': role.get('role_status'),
            'tag': role.get('tag'),
            'category': role.get('category'),
            'tooltip': role.get('tooltip'),
            'creator_id': 'americold_compass',
            'create_date': role.get('create_date'),
            'last_updater_id': 'americold_compass',
            'last_update_date': role.get('last_update_date')
        }
        cursor.execute("""
            MERGE INTO CFG_ROLE tgt
            USING (SELECT :role_id AS role_id FROM dual) src
            ON (tgt.ROLEID = src.role_id)
            WHEN MATCHED THEN UPDATE SET
                ROLENAME = :role_name,
                ROLEDESCRIPTION = :role_description,
                ROLESTATUS = :role_status,
                TAG = :tag,
                CATEGORY = :category,
                TOOLTIP = :tooltip,
                MODIFIEDBY = :last_updater_id,
                MODIFIEDDATETIME = :last_update_date
            WHEN NOT MATCHED THEN INSERT (
                ROLEID, ROLENAME, ROLEDESCRIPTION, ROLESTATUS, TAG, CATEGORY, TOOLTIP,
                MODIFIEDBY, MODIFIEDDATETIME
            ) VALUES (
                :role_id, :role_name, :role_description, :role_status, :tag, :category, :tooltip,
                :creator_id, :create_date
            )
        """, params)
        print(f"Oracle role {role_id} merged.")
    elif operation_type == 'delete' or is_deleted:
        cursor.execute("DELETE FROM CFG_ROLE WHERE ROLEID = :id", {'id': role_id})
        print(f"Oracle role {role_id} deleted.")

# Event processor
def process_event(record, redshift_cursor, oracle_cursor):
    event_data = record.get('event', {})
    operation_type = event_data.get('operationType')
    full_document = event_data.get('fullDocument', {})
    document_key = event_data.get('documentKey', {})

    if operation_type == 'insert':
        insert_redshift_role(redshift_cursor, full_document)
        update_oracle_role(oracle_cursor, full_document, operation_type)
    elif operation_type == 'update' or operation_type == 'replace':
        update_redshift_role(redshift_cursor, full_document)
        update_oracle_role(oracle_cursor, full_document, operation_type)
    elif operation_type == 'delete':
        # delete_redshift_role(redshift_cursor, document_key)
        update_oracle_role(oracle_cursor, document_key, operation_type)
    else:
        print(f"Unsupported operation: {operation_type}")

# Lambda handler
def lambda_handler(event, context):
    redshift_secret = get_secret('RedshiftDEV')
    oracle_secret = get_secret('Oracle-DB-AMC')

    # Connect to Redshift
    redshift_conn = psycopg2.connect(
        dbname=redshift_secret['dbname'],
        user=redshift_secret['username'],
        password=redshift_secret['password'],
        host=redshift_secret['host'],
        port=redshift_secret['port']
    )
    redshift_cursor = redshift_conn.cursor()

    # Connect to Oracle
    oracle_conn = oracledb.connect(
        user=oracle_secret['username'],
        password=oracle_secret['password'],
        # host=oracle_secret['host'],
        host='LVEXACCT-SCAN',
        port=oracle_secret['port'],
        service_name=oracle_secret['dbname']
    )
    oracle_cursor = oracle_conn.cursor()

    try:
        print('processing records')
        # print(record)        
        for record in event.get('events', []):
            print('processing record')
            print(record)
            process_event(record, redshift_cursor, oracle_cursor)
        redshift_conn.commit()
        oracle_conn.commit()
    except Exception as e:
        print(f"ERROR during processing: {e}")
        redshift_conn.rollback()
        oracle_conn.rollback()
    finally:
        redshift_cursor.close()
        redshift_conn.close()
        oracle_cursor.close()
        oracle_conn.close()
        print("Connections closed.")

    return 'OK'

