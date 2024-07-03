package db

import (
	"database/sql"
	"fmt"
	"log"
)

var (
	postgresHost     string = "127.0.0.1"
	postgresPort     int    = 5432
	postgresUsername string = "admin"
	postgresPassword string = "123"
	postgresDBName   string = "postgres"
	postgresSSLMode  string = "disable"
	postgresClient   *sql.DB
)

func PostgresClient() *sql.DB {
	if postgresClient != nil {
		return postgresClient
	}
	var err error
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		postgresHost, postgresPort, postgresUsername,
		postgresPassword, postgresDBName, postgresSSLMode,
	)
	postgresClient, err = sql.Open("postgres", connStr)

	if err != nil {
		log.Fatalln("invalid config for postgres connection:", err)
	}

	err = postgresClient.Ping()

	if err != nil {
		log.Fatalln("can't ping postgres db:", err)
	}

	return postgresClient
}
