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
	OldData   *json.RawMessage `json:"old_data,omitempty"` // pointer to handle nulls
	NewData   *json.RawMessage `json:"new_data,omitempty"` // pointer to handle nulls
	Timestamp string          `json:"timestamp"`
}

// initialize the DB connection
func initDB() error {
	var err error
	connStr := "user= password= dbname=" + dbName + " sslmode=disable" // ENTER DATABASE DETAILS HERE
	dbConn, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to the database: %v", err)
	}
	return nil
}

// fetch table names from the original database 
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

// applies the deltas to the restored database
func RestoreDatabase() error {
	
	// open connection
	restoredConnStr := "user= password= dbname=" + restoreDB + " sslmode=disable" // ENTER DETAILS HEREE
	restoredConn, err := sql.Open("postgres", restoredConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to the restored database: %v", err)
	}
	defer restoredConn.Close()

	// fetch all deltas from the deltas table, ordered by timestamp
	rows, err := dbConn.Query("SELECT action, table_name, old_data, new_data FROM deltas ORDER BY timestamp")
	if err != nil {
		return fmt.Errorf("error fetching deltas: %v", err)
	}
	defer rows.Close()

	// iterate over the deltas and apply each change to the restored database
	for rows.Next() {
		var delta Delta
		
		// use pointer in case of nulls
		if err := rows.Scan(&delta.Action, &delta.TableName, &delta.OldData, &delta.NewData); err != nil {
			return fmt.Errorf("error scanning delta: %v", err)
		}

		// build restored table name
		restoreTable := fmt.Sprintf("%s", delta.TableName)

		// just make sure restored tablae doesn't exist
		if !tableExists(restoredConn, restoreTable) {
			log.Printf("Skipping delta for non-existent table %s in the restored database", restoreTable)
			continue
		}

		// for each action, have a different delta
		switch delta.Action {
		case "INSERT":
			var newData map[string]interface{}
			if err := json.Unmarshal(*delta.NewData, &newData); err != nil {
				return fmt.Errorf("error unmarshalling new_data: %v", err)
			}

			// then just insert that delta into the restored table
			_, err := restoredConn.Exec(fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES ($1, $2, $3)", restoreTable), newData["id"], newData["name"], newData["age"])
			
			// format query
			query := fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES ($1, $2, $3)", restoreTable)

			// print query and values
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

			// update data in appropiate restored table
			_, err := restoredConn.Exec(fmt.Sprintf("UPDATE %s SET name = $1, age = $2 WHERE id = $3", restoreTable), newData["name"], newData["age"], oldData["id"])
			if err != nil {
				return fmt.Errorf("error applying update: %v", err)
			}

			// format query
			updateQuery := fmt.Sprintf("UPDATE %s SET name = $1, age = $2 WHERE id = $3", restoreTable)

			// print query and values
			fmt.Printf("Executing query: %s\n", updateQuery)
			fmt.Printf("        With values: name = %v, age = %v, id = %v\n", newData["name"], newData["age"], oldData["id"])



		case "DELETE":
			var oldData map[string]interface{}
			if delta.OldData != nil {
				if err := json.Unmarshal(*delta.OldData, &oldData); err != nil {
					return fmt.Errorf("error unmarshalling old_data: %v", err)
				}
			}

			// delete from restore table
			_, err := restoredConn.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", restoreTable), oldData["id"])
			if err != nil {
				return fmt.Errorf("error applying delete: %v", err)
			}

			// format query
			deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = $1", restoreTable)

			// print query and values
			fmt.Printf("Executing query: %s\n", deleteQuery)
			fmt.Printf("        With values: id = %v\n", oldData["id"])
		}
	}

	return nil
}

// check if a table exists in the restored database 
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
	
	// initialize the database connection to the original database
	if err := initDB(); err != nil {
		log.Fatalf("Error initializing DB: %v", err)
	}
	defer dbConn.Close()

	// fetch the list of tables in the original database 
	tables, err := getTableNames()
	if err != nil {
		log.Fatalf("Error fetching table names: %v", err)
	}

	log.Printf("Restoring tables: %v", tables)

	// call the restore function to apply deltas from the original database
	if err := RestoreDatabase(); err != nil {
		log.Fatalf("Error restoring database: %v", err)
	}

	log.Println("Database has been restored successfully.")
}
