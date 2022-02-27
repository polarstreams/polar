package localdb

import (
	"database/sql"
	"sync/atomic"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// Client represents a local db client.
type Client interface {
	Initializer
	Closer

	// Determines whether the local db was not present and had to be created
	DbWasNewlyCreated() bool

	CommitGeneration(gen1 *Generation, gen2 *Generation) error

	// Stores the group offset for a topic and token+index
	SaveOffset(offsetKv *OffsetStoreKeyValue) error

	// Retrieves all the stored offsets
	Offsets() ([]OffsetStoreKeyValue, error)

	// Gets latest generation stored per token
	LatestGenerations() ([]Generation, error)

	// Gets the following (children) generations
	GenerationsByParent(gen *Generation) ([]Generation, error)

	// Gets the last two (more recent first) stored generation by start token
	GetGenerationsByToken(token Token) ([]Generation, error)

	// Gets the generation by token and version, returns nil when not found
	GenerationInfo(token Token, version GenVersion) (*Generation, error)

	// Determines whether the localdb is being closed as a result of an application shutting down
	IsShuttingDown() bool
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
	config       conf.LocalDbConfig
	dbIsNew      bool
	db           *sql.DB
	queries      queries
	shuttingDown int32
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

func (c *client) IsShuttingDown() bool {
	return atomic.LoadInt32(&c.shuttingDown) == 1
}

func (c *client) Close() {
	atomic.StoreInt32(&c.shuttingDown, 1)
	_ = c.queries.selectGenerationsByToken.Close()
	_ = c.queries.selectGenerationsAll.Close()
	_ = c.queries.selectGenerationsByParent.Close()
	_ = c.queries.selectGeneration.Close()
	_ = c.queries.insertGeneration.Close()
	_ = c.queries.insertTransaction.Close()
	_ = c.queries.insertOffset.Close()
	log.Err(c.db.Close()).Msg("Local db closed")
}
