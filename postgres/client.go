package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/lib/pq" // Import the PostgreSQL driver package
	"github.com/spf13/viper"
)

// Client is a PostgreSQL client
type Client struct {
	db *sql.DB
}

// NewClient creates a new PostgreSQL client
func NewClient() (*Client, error) {
	host := viper.GetString("postgres.host")
	port := viper.GetString("postgres.port")
	dbname := viper.GetString("postgres.dbname")
	user := viper.GetString("postgres.user")
	password := viper.GetString("postgres.password")
	connectionString := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable", host, port, dbname, user, password)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Check if the connection is successful
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %w", err)
	}

	return &Client{db: db}, nil
}

// Close closes the PostgreSQL client connection
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// InsertData inserts the data into the PostgreSQL database
func (c *Client) InsertCsvData(data map[string]interface{}) error {
	query := "INSERT INTO csv_data (id, first_name, last_name, email_address, " +
		"created_at, deleted_at, merged_at, parent_user_id) VALUES" +
		" ($1, $2, $3, $4, $5, $6, $7, $8)"
	// Prepare the SQL statement
	stmt, err := c.db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	// Extract the values from the data map
	value1 := data["id"]
	value2 := data["first_name"]
	value3 := data["last_name"]
	value4 := data["email_address"]

	// Parse the string as a float64
	value5 := convertFloatTimestamp(data["created_at"].(string))
	value6 := convertFloatTimestamp(data["deleted_at"].(string))
	value7 := convertFloatTimestamp(data["merged_at"].(string))
	value8 := data["parent_user_id"]
	if value8 == "-1" {
		value8 = nil
	}
	// Execute the SQL statement with the values
	_, err = stmt.Exec(value1, value2, value3, value4, value5, value6, value7, value8)
	if err != nil {
		return fmt.Errorf("failed to execute SQL statement: %w", err)
	}

	return nil
}

// GetData retrieves data from the PostgreSQL database based on the provided filters, limit, and offset.
func (c *Client) GetData(filters map[string]interface{}, limit, offset int) ([]map[string]interface{}, error) {
	query := "SELECT id, first_name, last_name, email_address, created_at, deleted_at, merged_at, parent_user_id FROM csv_data WHERE 1=1"
	var args []interface{}

	// Add filters to the query
	i := 1
	for key, value := range filters {
		if value != nil {
			query += fmt.Sprintf(" AND %s = $%d", key, i)
			args = append(args, value)
			i++
		}
	}

	// Add limit and offset to the query
	query += " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset)
	// args = append(args, limit, offset)
	log.Println(query)
	log.Println(args...)

	// Prepare the SQL statement
	stmt, err := c.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	// Execute the SQL statement with the provided values
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL statement: %w", err)
	}
	defer rows.Close()

	// Fetch the result rows
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch result columns: %w", err)
	}

	// Create a slice to hold the result data
	var results []map[string]interface{}

	// Iterate over the rows and map the column values to a map
	for rows.Next() {
		// Create a map to hold the column values for each row
		rowData := make(map[string]interface{})

		// Create a slice to hold the column pointers
		columnPointers := make([]interface{}, len(columns))
		for i := range columns {
			columnPointers[i] = new(interface{})
		}

		// Scan the row and store the column values in the map
		err := rows.Scan(columnPointers...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Iterate over the column pointers and map the values to the rowData map
		for i, column := range columns {
			rowData[column] = *(columnPointers[i].(*interface{}))
		}

		// Append the rowData map to the results slice
		results = append(results, rowData)
	}

	// Check for any errors during row iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed during row iteration: %w", err)
	}

	return results, nil
}

func convertFloatTimestamp(timestamp string) *string {
	// Convert the timestamp value to a float64
	f, err := strconv.ParseFloat(timestamp, 64)
	if err != nil {
		return nil
	}

	// Check if the timestamp is -1
	if f == -1 {
		return nil
	}

	// Convert the float64 to a time.Time
	t := time.Unix(int64(f)/1000, 0) // Divide by 1000 to convert from milliseconds to seconds

	// Format the time as a string
	timestampStr := t.Format("2006-01-02 15:04:05")

	return &timestampStr
}
