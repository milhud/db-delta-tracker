package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

var (
	dbConn    *sql.DB // initialize database connection
	dbName    = "" // ENTER DATABASE NAME
	restoreDB = fmt.Sprintf("%s_restored", dbName) // restored database name
)

type Delta struct {
	Action    string          `json:"action"`
	TableName string          `json:"table_name"`
	OldData   *json.RawMessage `json:"old_data,omitempty"` // Pointer to handle NULL values
	NewData   *json.RawMessage `json:"new_data,omitempty"` // Pointer to handle NULL values
	Timestamp string          `json:"timestamp"`
}

// Initialize the DB connection
func initDB() error {
	var err error
	connStr := "user= password= dbname=" + dbName + " sslmode=disable" // ENTER DATABASE DETAILS HERE
	dbConn, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to the database: %v", err)
	}
	return nil
}

// Fetch table names from the original database 
func getTableNames() ([]string, error) {
	var tables []string
	rows, err := dbConn.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table names: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %v", err)
	}

	return tables, nil
}

// RestoreDatabase applies deltas to the restored database
func RestoreDatabase() error {
	// Open a connection to the restored database
	restoredConnStr := "user= password= dbname=" + restoreDB + " sslmode=disable" // ENTER DETAILS HEREE
	restoredConn, err := sql.Open("postgres", restoredConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to the restored database: %v", err)
	}
	defer restoredConn.Close()

	// Fetch all deltas from the deltas table, ordered by timestamp
	rows, err := dbConn.Query("SELECT action, table_name, old_data, new_data FROM deltas ORDER BY timestamp")
	if err != nil {
		return fmt.Errorf("error fetching deltas: %v", err)
	}
	defer rows.Close()

	// Iterate over the deltas and apply each change to the restored database
	for rows.Next() {
		var delta Delta
		// Use a pointer to json.RawMessage to handle potential NULL values
		if err := rows.Scan(&delta.Action, &delta.TableName, &delta.OldData, &delta.NewData); err != nil {
			return fmt.Errorf("error scanning delta: %v", err)
		}

		// Build the restored table name dynamically
		restoreTable := fmt.Sprintf("%s", delta.TableName)

		// Check if the restored table exists
		if !tableExists(restoredConn, restoreTable) {
			log.Printf("Skipping delta for non-existent table %s in the restored database", restoreTable)
			continue
		}

		// Check and handle each action for the delta
		switch delta.Action {
		case "INSERT":
			var newData map[string]interface{}
			if err := json.Unmarshal(*delta.NewData, &newData); err != nil {
				return fmt.Errorf("error unmarshalling new_data: %v", err)
			}

			// Insert the data into the appropriate restored table
			_, err := restoredConn.Exec(fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES ($1, $2, $3)", restoreTable), newData["id"], newData["name"], newData["age"])
			
			// Format the query
			query := fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES ($1, $2, $3)", restoreTable)

			// Print the query and values
			fmt.Printf("Executing query: %s\n", query)
			fmt.Printf("         With values: id = %v, name = %v, age = %v\n", newData["id"], newData["name"], newData["age"])

			
			if err != nil {
				return fmt.Errorf("error applying insert: %v", err)
			}

		case "UPDATE":
			var oldData map[string]interface{}
			if delta.OldData != nil {
				if err := json.Unmarshal(*delta.OldData, &oldData); err != nil {
					return fmt.Errorf("error unmarshalling old_data: %v", err)
				}
			}

			var newData map[string]interface{}
			if delta.NewData != nil {
				if err := json.Unmarshal(*delta.NewData, &newData); err != nil {
					return fmt.Errorf("error unmarshalling new_data: %v", err)
				}
			}

			// Update the data in the appropriate restored table
			_, err := restoredConn.Exec(fmt.Sprintf("UPDATE %s SET name = $1, age = $2 WHERE id = $3", restoreTable), newData["name"], newData["age"], oldData["id"])
			if err != nil {
				return fmt.Errorf("error applying update: %v", err)
			}

			// Format the query
			updateQuery := fmt.Sprintf("UPDATE %s SET name = $1, age = $2 WHERE id = $3", restoreTable)

			// Print the query and values
			fmt.Printf("Executing query: %s\n", updateQuery)
			fmt.Printf("        With values: name = %v, age = %v, id = %v\n", newData["name"], newData["age"], oldData["id"])



		case "DELETE":
			var oldData map[string]interface{}
			if delta.OldData != nil {
				if err := json.Unmarshal(*delta.OldData, &oldData); err != nil {
					return fmt.Errorf("error unmarshalling old_data: %v", err)
				}
			}

			// Delete the data from the appropriate restored table
			_, err := restoredConn.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", restoreTable), oldData["id"])
			if err != nil {
				return fmt.Errorf("error applying delete: %v", err)
			}

			// Format the query
			deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = $1", restoreTable)

			// Print the query and values
			fmt.Printf("Executing query: %s\n", deleteQuery)
			fmt.Printf("        With values: id = %v\n", oldData["id"])
		}
	}

	return nil
}

// Check if a table exists in the restored database 
func tableExists(dbConn *sql.DB, tableName string) bool {
	var exists bool
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)`)
	err := dbConn.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		log.Printf("Error checking if table %s exists in restored database: %v", tableName, err)
		return false
	}
	return exists
}

func main() {
	// Initialize the database connection to the original database
	if err := initDB(); err != nil {
		log.Fatalf("Error initializing DB: %v", err)
	}
	defer dbConn.Close()

	// Fetch the list of tables in the original database 
	tables, err := getTableNames()
	if err != nil {
		log.Fatalf("Error fetching table names: %v", err)
	}

	log.Printf("Restoring tables: %v", tables)

	// Call the restore function to apply deltas from the original database
	if err := RestoreDatabase(); err != nil {
		log.Fatalf("Error restoring database: %v", err)
	}

	log.Println("Database has been restored successfully.")
}
