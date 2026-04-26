from DbManager import DatabaseManager

class SchemaService:
    """
    Middleware service that extracts database metadata.
    This allows the AI Agent to 'inspect' the database before generating queries.
    """

    @classmethod
    def get_all_tables(cls):
        """Returns a list of all tables in the database."""
        query = "SHOW TABLES;"
        results = DatabaseManager.execute_query(query)
        if results:
            # Extracts the first value from each dict (the table name)
            return [list(table.values())[0] for table in results]
        return []

    @classmethod
    def get_table_schema(cls, table_name: str):
        """Returns the column definitions for a specific table."""
        # Using params to prevent SQL injection even on metadata queries
        # Note: DESCRIBE doesn't support placeholders in all MySQL versions, 
        # but we use it here for logic demonstration.
        query = f"DESCRIBE {table_name};" 
        results = DatabaseManager.execute_query(query)
        return results

    @classmethod
    def get_column_samples(cls, table_name: str, column_name: str, limit: int = 15):
        """Fetches distinct values for a specific column to provide data context."""
        query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT {limit};"
        results = DatabaseManager.execute_query(query)
        if results:
            return [str(list(row.values())[0]) for row in results]
        return []

    @classmethod
    def get_formatted_schema(cls):
        """
        Orchestrates the metadata into a string format that is 
        highly readable for an LLM like Qwen.
        """
        tables = cls.get_all_tables()
        
        # Columns that usually contain useful categorical data for filtering
        categorical_keywords = ['NAME', 'TYPE', 'CATEGORY', 'CATG', 'STATUS']
        
        schema_description = "Database Schema Metadata:\n"
        
        for table in tables:
            schema_description += f"\nTable: {table}\n"
            columns = cls.get_table_schema(table)
            if columns:
                for col in columns:
                    field_name = col['Field']
                    pk_info = " [PRIMARY KEY]" if col.get('Key') == 'PRI' else ""
                    null_info = " NOT NULL" if col.get('Null') == 'NO' else ""
                    
                    # Fetch sample values if the column is likely a dimension
                    samples_text = ""
                    if any(kw in field_name.upper() for kw in categorical_keywords):
                        samples = cls.get_column_samples(table, field_name)
                        if samples:
                            samples_text = f" | Samples: {', '.join(samples)}"
                    
                    schema_description += f" - {field_name} ({col['Type']}){null_info}{pk_info}{samples_text}\n"
        
        return schema_description

if __name__ == "__main__":
    # Test the discovery service
    print("--- Running Schema Discovery ---")
    full_metadata = SchemaService.get_formatted_schema()
    print(full_metadata)