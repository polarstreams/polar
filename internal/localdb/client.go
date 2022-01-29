package localdb

import (
	"database/sql"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// Client represents a local db client.
type Client interface {
	Initializer
	Closer

	// Determines whether the local db was not present and had to be created
	DbWasNewlyCreated() bool

	CommitGeneration(generation *Generation) error

	// Stores the group offset for a topic and token+index
	SaveOffset(offsetKv *OffsetStoreKeyValue) error

	// Retrieves all the stored offsets
	Offsets() ([]OffsetStoreKeyValue, error)

	// TODO: convert to history
	GetGenerationsByToken(token Token) ([]Generation, error)
}

// NewClient creates a new instance of Client.
func NewClient(config conf.LocalDbConfig) Client {
	return &client{
		config:  config,
		dbIsNew: false,
		queries: queries{},
	}
}

type client struct {
	config  conf.LocalDbConfig
	dbIsNew bool
	db      *sql.DB
	queries queries
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

	existing, err := c.hasLocalInfo()
	if err != nil {
		return err
	}

	if existing {
		if err := c.setCurrentSchemaVersion(); err != nil {
			return err
		}
	} else {
		c.dbIsNew = true
		if err := c.createLocalInfo(); err != nil {
			return nil
		}
	}

	c.prepareQueries()
	return nil
}

func (c *client) DbWasNewlyCreated() bool {
	return c.dbIsNew
}

func (c *client) Close() {
	_ = c.queries.selectGenerations.Close()
	log.Err(c.db.Close()).Msg("Local db closed")
}
