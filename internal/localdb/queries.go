package localdb

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
)

type queries struct {
	selectGenerations *sql.Stmt
	insertGeneration  *sql.Stmt
	insertTransaction *sql.Stmt
	selectOffsets     *sql.Stmt
	insertOffset      *sql.Stmt
}

func (c *client) prepareQueries() {
	c.queries.selectGenerations = c.prepare(`
		SELECT start_token, end_token, version, timestamp, tx, tx_leader, status, leader, followers FROM
		generations WHERE start_token = ? ORDER BY start_token, version DESC LIMIT 2`)

	c.queries.insertGeneration = c.prepare(`
		INSERT INTO generations (start_token, end_token, version, timestamp, tx, tx_leader, status, leader, followers)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)

	c.queries.insertTransaction = c.prepare(
		`INSERT INTO transactions (tx, origin, timestamp, status) VALUES (?, ?, ?, ?)`)

	c.queries.insertOffset = c.prepare(
		`INSERT INTO offsets (group_name, topic, token, range_index, version, offset, source)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`)

	c.queries.selectOffsets = c.prepare(
		`SELECT group_name, topic, token, range_index, version, offset, source FROM offsets`)
}

func (c *client) prepare(query string) *sql.Stmt {
	stmt, err := c.db.Prepare(query)
	panicIfError(err)
	return stmt
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

// hasLocalInfo returns true when the local_info table has information
func (c *client) hasLocalInfo() (bool, error) {
	if rows, err := c.db.Query("SELECT key FROM local_info WHERE key='local'"); err != nil {
		return false, err
	} else {
		defer rows.Close()
		for rows.Next() {
			return true, nil
		}
		return false, nil
	}
}

func (c *client) createLocalInfo() error {
	q := "INSERT INTO local_info (key, schema_version_init, schema_version_current) VALUES (?, ?, ?)"
	_, err := c.db.Exec(q, "local", "1.0", "1.0")
	return err
}

func (c *client) setCurrentSchemaVersion() error {
	q := "UPDATE local_info SET schema_version_current = ? WHERE key = ?"
	_, err := c.db.Exec(q, "1.0", "local")
	return err
}

func (c *client) GetGenerationsByToken(token Token) ([]Generation, error) {
	rows, err := c.queries.selectGenerations.Query(int64(token))
	if err != nil {
		return nil, err
	}

	result := make([]Generation, 0)
	defer rows.Close()
	for rows.Next() {
		item := Generation{}
		var followers string
		err = rows.Scan(
			&item.Start, &item.End, &item.Version, &item.Timestamp, &item.Tx, &item.TxLeader,
			&item.Status, &item.Leader, &followers)
		if err != nil {
			return result, err
		}
		if followers != "" {
			parts := strings.Split(followers, ",")
			followersSlice := make([]int, len(parts))
			for i := range followersSlice {
				if followersSlice[i], err = strconv.Atoi(parts[i]); err != nil {
					return nil, err
				}
			}

			item.Followers = followersSlice
		}
		result = append(result, item)
	}
	return result, nil
}

func (c *client) CommitGeneration(gen *Generation) error {
	db := c.db
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// The rollback will be ignored if the tx has been committed
	defer tx.Rollback()

	insertGenStatement := tx.StmtContext(context.TODO(), c.queries.insertGeneration)
	insertTxStatement := tx.StmtContext(context.TODO(), c.queries.insertTransaction)

	if _, err := insertTxStatement.Exec(
		gen.Tx, gen.TxLeader, gen.Timestamp, StatusCommitted); err != nil {
		return err
	}

	followers := utils.ToCsv(gen.Followers)
	if _, err := insertGenStatement.Exec(
		gen.Start, gen.End, gen.Version, gen.Timestamp, gen.Tx, gen.TxLeader,
		StatusCommitted, gen.Leader, followers); err != nil {
		return err
	}

	return tx.Commit()
}

func (c *client) SaveOffset(kv *OffsetStoreKeyValue) error {
	key := kv.Key
	value := kv.Value
	_, err := c.queries.
		insertOffset.
		Exec(key.Group, key.Topic, key.Token, key.RangeIndex, value.Version, value.Offset, value.Source)
	return err
}

func (c *client) Offsets() ([]OffsetStoreKeyValue, error) {
	rows, err := c.queries.selectOffsets.Query()
	if err != nil {
		return nil, err
	}

	result := make([]OffsetStoreKeyValue, 0)
	defer rows.Close()
	for rows.Next() {
		kv := OffsetStoreKeyValue{}
		err = rows.Scan(
			&kv.Key.Group, &kv.Key.Topic, &kv.Key.Token, &kv.Key.RangeIndex, &kv.Value.Version, &kv.Value.Offset,
			&kv.Value.Source)
		if err != nil {
			return result, err
		}
		result = append(result, kv)
	}
	return result, nil
}
