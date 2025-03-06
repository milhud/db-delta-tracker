package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	_ "github.com/lib/pq"
)

var (
	dbConn        *sql.DB
	originalDB    *sql.DB
	dbName        = ""
	restoreDB     = fmt.Sprintf("%s_restored", dbName)
)

// initialize the DB connection to the default "postgres" database
func initDB() error {
	var err error
	connStr := "user= password= dbname= sslmode=disable" // MUST FILL IN USERNAME AND PASSWORD
	dbConn, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to the database: %v", err)
	}

	// create the deltas table in the original database
	if err := createDeltasTable(); err != nil {
		return fmt.Errorf("failed to create deltas table: %v", err)
	}

	// add triggers to all tables in the original database
	if err := addTriggersToTables(); err != nil {
		return fmt.Errorf("failed to add triggers to tables: %v", err)
	}

	log.Println("-The deltas table and triggers have been succesfully created for the database-")
	return nil
}

// create the deltas table (if it doesn't exist)
func createDeltasTable() error {
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS deltas (
		id SERIAL PRIMARY KEY,
		action VARCHAR(10),
		table_name VARCHAR(100),
		old_data JSONB,
		new_data JSONB,
		timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := dbConn.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create deltas table: %v", err)
	}
	log.Println("Deltas table created (or already exists).")
	return nil
}

// add triggers to track changes in all tables in the original database
func addTriggersToTables() error {
	
	// query to get all tables in the testdatabase
	tablesQuery := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
	rows, err := dbConn.Query(tablesQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch tables from database: %v", err)
	}
	defer rows.Close()

	// add triggers for each table in testdatabase
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}

		// skip the 'deltas' table (tracking triggers just in other databases)
		if tableName == "deltas" {
			continue
		}

		// create trigger function for INSERT, UPDATE, DELETE actions
		triggerFuncQuery := fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION log_%s_changes() RETURNS TRIGGER AS $$
		BEGIN
			-- Log INSERT action
			IF (TG_OP = 'INSERT') THEN
				INSERT INTO deltas (action, table_name, new_data)
				VALUES ('INSERT', TG_TABLE_NAME, row_to_json(NEW));
				RETURN NEW;
			END IF;

			-- Log UPDATE action
			IF (TG_OP = 'UPDATE') THEN
				INSERT INTO deltas (action, table_name, old_data, new_data)
				VALUES ('UPDATE', TG_TABLE_NAME, row_to_json(OLD), row_to_json(NEW));
				RETURN NEW;
			END IF;

			-- Log DELETE action
			IF (TG_OP = 'DELETE') THEN
				INSERT INTO deltas (action, table_name, old_data)
				VALUES ('DELETE', TG_TABLE_NAME, row_to_json(OLD));
				RETURN OLD;
			END IF;

			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
		`, tableName)

		_, err := dbConn.Exec(triggerFuncQuery)
		if err != nil {
			return fmt.Errorf("failed to create trigger function for table %s: %v", tableName, err)
		}

		// create the trigger that calls the above function
		triggerQuery := fmt.Sprintf(`
		CREATE TRIGGER %s_trigger
		AFTER INSERT OR UPDATE OR DELETE ON %s
		FOR EACH ROW EXECUTE FUNCTION log_%s_changes();
		`, tableName, tableName, tableName)

		_, err = dbConn.Exec(triggerQuery)
		if err != nil {
			return fmt.Errorf("failed to create trigger for table %s: %v", tableName, err)
		}

		log.Printf("Trigger added to table %s.", tableName)
	}

	return nil
}

// reconnect to a specified database
func reconnectToDatabase(dbName string) (*sql.DB, error) {
	connStr := fmt.Sprintf("user=postgres password=password dbname=%s sslmode=disable", dbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to reconnect to database %s: %v", dbName, err)
	}
	return db, nil
}

// check if the restored database exists, and create it if it doesn't
func createRestoredDatabase() error {
	var exists bool
	err := dbConn.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", restoreDB).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if the restored database exists: %v", err)
	}

	if exists {
		log.Printf("Database %s already exists, skipping creation.", restoreDB)
		return nil
	}

	_, err = dbConn.Exec(fmt.Sprintf("CREATE DATABASE %s;", restoreDB))
	if err != nil {
		return fmt.Errorf("failed to create restored database %s: %v", restoreDB, err)
	}

	log.Printf("Database %s created successfully.", restoreDB)
	return nil
}

// backup a table as a JSON file
func backupTable(tableName string) error {
	
	// connect to the original database
	originalDB, err := reconnectToDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to reconnect to original database: %v", err)
	}
	defer originalDB.Close()

	// query to fetch all rows from the table
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := originalDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to fetch data from table %s: %v", tableName, err)
	}
	defer rows.Close()

	// get columns for the table
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s: %v", tableName, err)
	}

	var allRows []map[string]interface{}
	for rows.Next() {
		
		// create a slice to hold the column values
		columnsValues := make([]interface{}, len(columns))
		for i := range columnsValues {
			columnsValues[i] = new(interface{})
		}

		// scan the row into the slice
		err := rows.Scan(columnsValues...)
		if err != nil {
			return fmt.Errorf("failed to scan row from table %s: %v", tableName, err)
		}

		// map the column names to the corresponding values
		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := *(columnsValues[i].(*interface{}))
			rowMap[colName] = val
		}

		// add the row map to the allRows slice
		allRows = append(allRows, rowMap)
	}

	// serialize the rows to JSON
	fileName := fmt.Sprintf("%s.json", tableName)
	data, err := json.Marshal(allRows)
	if err != nil {
		return fmt.Errorf("failed to serialize data to JSON for table %s: %v", tableName, err)
	}

	// write the JSON data to a file
	err = ioutil.WriteFile(fileName, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write JSON data for table %s: %v", tableName, err)
	}

	log.Printf("Table %s successfully backed up as JSON.", tableName)
	return nil
}

// restore a table from a JSON file
func restoreTable(tableName string) error {
	
	// connect to the restored database
	restoredDB, err := reconnectToDatabase(restoreDB)
	if err != nil {
		return fmt.Errorf("failed to reconnect to restored database: %v", err)
	}
	defer restoredDB.Close()

	// read the JSON file containing the backup data
	fileName := fmt.Sprintf("%s.json", tableName)
	fileData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("failed to read JSON file for table %s: %v", tableName, err)
	}

	// deserialize the JSON data
	var rows []map[string]interface{}
	err = json.Unmarshal(fileData, &rows)
	if err != nil {
		return fmt.Errorf("failed to deserialize JSON data for table %s: %v", tableName, err)
	}

	// create the table in the restored database (assuming schema matches)
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			age INT
		);`, tableName)
	_, err = restoredDB.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create restored table %s: %v", tableName, err)
	}

	// prepare the insert query based on the columns in the table
	// 		assuming a simple table schema for now; adjust as needed
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES ($1, $2, $3)", tableName)

	// insert each row into the restored table
	for _, row := range rows {
		_, err := restoredDB.Exec(insertQuery, row["id"], row["name"], row["age"])
		if err != nil {
			return fmt.Errorf("failed to insert data into restored table %s: %v", tableName, err)
		}
	}

	log.Printf("Table %s successfully restored from JSON.", tableName)
	return nil
}

// backup and restore all tables
func backupAndRestoreTables() error {
	// connect to the original database
	originalDB, err := reconnectToDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to reconnect to original database: %v", err)
	}
	defer originalDB.Close()

	// connect to the restored database
	restoredDB, err := reconnectToDatabase(restoreDB)
	if err != nil {
		return fmt.Errorf("failed to connect to restored database: %v", err)
	}
	defer restoredDB.Close()

	// fetch the list of tables to backup
	tablesQuery := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
	rows, err := originalDB.Query(tablesQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch tables from original database: %v", err)
	}
	defer rows.Close()

	// backup and restore each table
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}

		// Backup and restore the table
		if err := backupTable(tableName); err != nil {
			return fmt.Errorf("failed to backup table %s: %v", tableName, err)
		}
		if err := restoreTable(tableName); err != nil {
			return fmt.Errorf("failed to restore table %s: %v", tableName, err)
		}
	}

	log.Println("Backup and restore completed successfully.")
	return nil
}

func main() {
	// initialize database connections
	err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize the database: %v", err)
	}

	// create the restored database
	err = createRestoredDatabase()
	if err != nil {
		log.Fatalf("Failed to create restored database: %v", err)
	}

	// backup and restore all tables
	err = backupAndRestoreTables()
	if err != nil {
		log.Fatalf("Backup and restore failed: %v", err)
	}

	log.Println("All tables backed up and restored successfully.")
}
