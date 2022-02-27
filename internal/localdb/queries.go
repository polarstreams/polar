package localdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
)

type queries struct {
	selectGenerationsByToken  *sql.Stmt
	selectGenerationsAll      *sql.Stmt
	selectGenerationsByParent *sql.Stmt
	selectGeneration          *sql.Stmt
	insertGeneration          *sql.Stmt
	insertTransaction         *sql.Stmt
	selectOffsets             *sql.Stmt
	insertOffset              *sql.Stmt
}

func (c *client) prepareQueries() {
	const generationColumns = "start_token, end_token, version, timestamp, tx, tx_leader, status, leader, followers, parents"

	c.queries.selectGenerationsByToken = c.prepare(fmt.Sprintf(`
		SELECT %s FROM generations
		WHERE start_token = ? ORDER BY start_token, version DESC LIMIT 2`, generationColumns))

	c.queries.selectGenerationsByParent = c.prepare(fmt.Sprintf(`
		SELECT %s FROM generations
		WHERE start_token >= ? AND end_token <= ? AND parents = ?`, generationColumns))

	c.queries.selectGenerationsAll = c.prepare(fmt.Sprintf(`
		SELECT %s
		FROM
			generations
			INNER JOIN (SELECT start_token AS t, MAX(version) as v FROM generations GROUP BY start_token) AS max_gen
				ON max_gen.t = generations.start_token AND max_gen.v = generations.version`,
		generationColumns))

	c.queries.selectGeneration = c.prepare(fmt.Sprintf(
		`SELECT %s FROM generations WHERE start_token = ? AND version = ?`, generationColumns))

	c.queries.insertGeneration = c.prepare(fmt.Sprintf(
		`INSERT INTO generations (%s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, generationColumns))

	c.queries.insertTransaction = c.prepare(
		`INSERT INTO transactions (tx, origin, timestamp, status) VALUES (?, ?, ?, ?)`)

	c.queries.insertOffset = c.prepare(
		`REPLACE INTO offsets (group_name, topic, token, range_index, version, offset, source)
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
	rows, err := c.queries.selectGenerationsByToken.Query(int64(token))
	if err != nil {
		return nil, err
	}

	result := make([]Generation, 0)
	defer rows.Close()
	for rows.Next() {
		item, err := scanGenRow(rows)
		if err != nil {
			return result, err
		}
		result = append(result, *item)
	}
	return result, nil
}

func (c *client) GenerationsByParent(gen *Generation) ([]Generation, error) {
	// Look for the children of the current generation
	parent := parentsToString([]GenParent{{Start: gen.Start, Version: gen.Version}})

	rows, err := c.queries.selectGenerationsByParent.Query(gen.Start, gen.End, parent)
	if err != nil {
		return nil, err
	}

	result := make([]Generation, 0)
	defer rows.Close()
	for rows.Next() {
		item, err := scanGenRow(rows)
		if err != nil {
			return result, err
		}
		result = append(result, *item)
	}
	return result, nil
}

func (c *client) LatestGenerations() ([]Generation, error) {
	rows, err := c.queries.selectGenerationsAll.Query()
	if err != nil {
		return nil, err
	}

	result := make([]Generation, 0)
	defer rows.Close()
	for rows.Next() {
		item, err := scanGenRow(rows)
		if err != nil {
			return result, err
		}
		result = append(result, *item)
	}
	return result, nil
}

func scanGenRow(rows *sql.Rows) (*Generation, error) {
	result := Generation{}
	var followers string
	var parents string
	err := rows.Scan(
		&result.Start, &result.End, &result.Version, &result.Timestamp, &result.Tx, &result.TxLeader,
		&result.Status, &result.Leader, &followers, &parents)
	if err != nil {
		return nil, err
	}
	if followers != "" {
		parts := strings.Split(followers, ",")
		followersSlice := make([]int, len(parts))
		for i := range followersSlice {
			if followersSlice[i], err = strconv.Atoi(parts[i]); err != nil {
				return nil, err
			}
		}

		result.Followers = followersSlice
	}
	result.Parents = parentsFromString(parents)
	return &result, nil
}

func (c *client) GenerationInfo(token Token, version GenVersion) (*Generation, error) {
	rows, err := c.queries.selectGeneration.Query(token, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	return scanGenRow(rows)
}

func (c *client) CommitGeneration(gen1 *Generation, gen2 *Generation) error {
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
		gen1.Tx, gen1.TxLeader, gen1.Timestamp, StatusCommitted); err != nil {
		return err
	}

	if err := execInsertGeneration(insertGenStatement, gen1); err != nil {
		return err
	}

	if gen2 != nil {
		if err := execInsertGeneration(insertGenStatement, gen2); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func execInsertGeneration(stmt *sql.Stmt, gen *Generation) error {
	_, err := stmt.Exec(
		gen.Start, gen.End, gen.Version, gen.Timestamp, gen.Tx, gen.TxLeader,
		StatusCommitted, gen.Leader, utils.ToCsv(gen.Followers), parentsToString(gen.Parents))
	return err
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

func parentsFromString(stringValue string) []GenParent {
	var result []GenParent
	utils.PanicIfErr(json.Unmarshal([]byte(stringValue), &result), "Unexpected error when deserializing parents")
	return result
}

func parentsToString(parents []GenParent) string {
	if len(parents) == 0 {
		return "[]"
	}
	bytes, err := json.Marshal(parents)
	utils.PanicIfErr(err, "Unexpected error when serializing parents")
	return string(bytes)
}
