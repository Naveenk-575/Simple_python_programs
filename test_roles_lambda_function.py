import json
import pytest
from unittest.mock import patch, MagicMock
from roles_lambda_function import (
    lambda_handler,
    extract_id,
    insert_redshift_role,
    update_redshift_role,
    update_oracle_role
)

# Sample documents
sample_doc = {
    "_id": {"$oid": "64a7f8e2c9e77b0012345678"},
    "role_id": "admin",
    "role_name": "Administrator",
    "role_description": "Full access",
    "role_status": "active",
    "tag": "core",
    "category": "system",
    "tooltip": "Admin role",
    "create_date": "2023-07-01T12:00:00Z",
    "last_update_date": "2023-07-01T12:00:00Z"
}

sample_doc_key = {
    "role_id": "admin"
}

# Mock secrets
mock_redshift_secret = {
    "dbname": "mockdb",
    "username": "mockuser",
    "password": "mockpass",
    "host": "localhost",
    "port": 5432
}

mock_oracle_secret = {
    "dbname": "mockservice",
    "username": "oracleuser",
    "password": "oraclepass",
    "host": "localhost",
    "port": 1521
}

# Mock cursor and connection
class MockCursor:
    def __init__(self):
        self.rowcount = 1

    def execute(self, query, params=None):
        print(f"Mock execute: {query}")
        if params:
            print(f"With params: {params}")

    def close(self):
        print("Mock cursor closed.")

class MockConnection:
    def cursor(self):
        return MockCursor()

    def commit(self):
        print("Mock commit.")

    def rollback(self):
        print("Mock rollback.")

    def close(self):
        print("Mock connection closed.")

@pytest.fixture
def mock_boto3_client():
    with patch("boto3.client") as mock_client:
        secrets_manager = MagicMock()
        secrets_manager.get_secret_value.side_effect = lambda SecretId: {
            "SecretString": json.dumps(mock_redshift_secret if SecretId == "RedshiftDEV" else mock_oracle_secret)
        }
        mock_client.return_value = secrets_manager
        yield mock_client

@pytest.fixture
def mock_db_connections():
    with patch("psycopg2.connect", return_value=MockConnection()), \
         patch("oracledb.connect", return_value=MockConnection()):
        yield

# Unit tests
def test_extract_id_with_oid():
    doc = {'_id': {'$oid': '12345'}}
    assert extract_id(doc) == '12345'

def test_extract_id_without_oid():
    doc = {'_id': '67890'}
    assert extract_id(doc) == '67890'

def test_extract_id_none():
    doc = {}
    assert extract_id(doc) is None

def test_insert_redshift_role_missing_id(capfd):
    cursor = MagicMock()
    role = {}
    insert_redshift_role(cursor, role)
    out, _ = capfd.readouterr()
    assert "Missing _id for insert." in out

def test_update_redshift_role_missing_id(capfd):
    cursor = MagicMock()
    role = {}
    update_redshift_role(cursor, role)
    out, _ = capfd.readouterr()
    assert "Missing _id for update." in out
def test_update_oracle_role_missing_role_id(capfd):
    cursor = MagicMock()
    role = {}
    update_oracle_role(cursor, role, 'insert')
    out, _ = capfd.readouterr()
    assert "Missing role_id for Oracle operation." in out
def test_update_oracle_role_delete_flag(capfd):
    cursor = MagicMock()
    role = {"role_id": "admin", "is_deleted": True}
    update_oracle_role(cursor, role, 'update')
    out, _ = capfd.readouterr()
    assert "Oracle role admin deleted." in out

# Lambda handler tests
def test_lambda_handler_insert(mock_boto3_client, mock_db_connections):
    event = {
        "events": [
            {"event": {"operationType": "insert", "fullDocument": sample_doc, "documentKey": sample_doc_key}}
        ]
    }
    result = lambda_handler(event, None)
    assert result == "OK"

def test_lambda_handler_update(mock_boto3_client, mock_db_connections):
    event = {
        "events": [
            {"event": {"operationType": "update", "fullDocument": sample_doc, "documentKey": sample_doc_key}}
        ]
    }
    result = lambda_handler(event, None)
    assert result == "OK"

def test_lambda_handler_replace(mock_boto3_client, mock_db_connections):
    event = {
        "events": [
            {"event": {"operationType": "replace", "fullDocument": sample_doc, "documentKey": sample_doc_key}}
        ]
    }
    result = lambda_handler(event, None)
    assert result == "OK"

def test_lambda_handler_delete(mock_boto3_client, mock_db_connections):
    event = {
        "events": [
            {"event": {"operationType": "delete", "fullDocument": {}, "documentKey": sample_doc_key}}
        ]
    }
    result = lambda_handler(event, None)
    assert result == "OK"

def test_lambda_handler_unsupported(mock_boto3_client, mock_db_connections, capfd):
    event = {
        "events": [
            {"event": {"operationType": "unknown", "fullDocument": sample_doc, "documentKey": sample_doc_key}}
        ]
    }
    result = lambda_handler(event, None)
    out, _ = capfd.readouterr()
    assert "Unsupported operation: unknown" in out
    assert result == "OK"
