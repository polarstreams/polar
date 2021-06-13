package localdb

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
)

type queries struct {
	selectGenerations *sql.Stmt
	updateGeneration  *sql.Stmt
	insertGeneration  *sql.Stmt
}

func (c *client) prepareQueries() {
	c.queries.selectGenerations = c.prepare(`
		SELECT start_token, end_token, version, tx, status, leader, followers FROM
		generations WHERE start_token = ? ORDER BY start_token, version DESC LIMIT 2`)

	c.queries.insertGeneration = c.prepare(`
		INSERT INTO generations (start_token, end_token, version, tx, status, leader, followers)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)

	c.queries.updateGeneration = c.prepare(`
		UPDATE generations
		SET
			end_token = ?, tx = ?, status = ?, leader = ?, followers = ?
		WHERE
			start_token = ? AND version = ?
			AND
			end_token = ? AND tx = ? AND status = ? AND leader = ? AND followers = ?`)
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

func (c *client) GetGenerationsByToken(token types.Token) ([]types.Generation, error) {
	rows, err := c.queries.selectGenerations.Query(int64(token))
	if err != nil {
		return nil, err
	}

	result := make([]types.Generation, 0)
	defer rows.Close()
	for rows.Next() {
		item := types.Generation{}
		var followers string
		if err = rows.Scan(&item.Start, &item.End, &item.Version, &item.Tx, &item.Status, &item.Leader, &followers); err != nil {
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

func (c *client) UpsertGeneration(token types.Token, existing *types.Generation, newGen *types.Generation) error {
	newFollowers := utils.ToCsv(newGen.Followers)
	if existing == nil {
		_, err := c.queries.insertGeneration.Exec(
			newGen.Start, newGen.End, newGen.Version, newGen.Tx, newGen.Status, newGen.Leader, newFollowers)
		return err
	}

	r, err := c.queries.updateGeneration.Exec(
		// SET
		newGen.End, newGen.Tx, newGen.Status, newGen.Leader, newFollowers,
		// WHERE
		newGen.Start, newGen.Version,
		// WHERE existing gen
		existing.End, existing.Tx, existing.Status, existing.Leader, utils.ToCsv(existing.Followers),
	)

	if err != nil {
		return err
	}

	if n, _ := r.RowsAffected(); n == 0 {
		return errors.New("No generation was updated")
	}

	return nil
}
