package localdb

import (
	"database/sql"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// Client represents a local db client.
type Client interface {
	types.Initializer
	types.Closer

	// Determines whether the local db was not present and had to be created
	DbWasNewlyCreated() bool
}

// NewClient creates a new instance of Client.
func NewClient(config conf.Config) Client {
	return &client{
		config:  config,
		dbIsNew: false,
	}
}

type client struct {
	config  conf.Config
	dbIsNew bool
	db      *sql.DB
}

func (c *client) Init() error {
	dbPath := c.config.LocalDbPath()
	log.Info().Msgf("Initializing local db from %s", dbPath)
	db, err := sql.Open("sqlite3", dbPath)

	if err != nil {
		return err
	}

	c.db = db

	if _, err := db.Exec(ddl01); err != nil {
		return err
	}

	return nil
}

func (c *client) DbWasNewlyCreated() bool {
	return c.dbIsNew
}

func (c *client) Close() {
	log.Err(c.db.Close()).Msg("Local db closed")
}
