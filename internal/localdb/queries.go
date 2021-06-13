package localdb

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/jorgebay/soda/internal/types"
)

type queries struct {
	selectGenerations *sql.Stmt
}

func (c *client) prepareQueries() {
	c.queries.selectGenerations = c.prepare(`
		SELECT token, version, tx, status, leader, followers FROM
		generations WHERE token = ? ORDER BY token, version DESC LIMIT 2`)
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

func (c *client) getGenerationPerToken(token types.Token) ([]types.Generation, error) {
	rows, err := c.queries.selectGenerations.Query(int64(token))
	if err != nil {
		return nil, err
	}

	result := make([]types.Generation, 0)
	defer rows.Close()
	for rows.Next() {
		item := types.Generation{}
		var followers string
		if err = rows.Scan(&item.Token, &item.Version, &item.Tx, &item.Status, &item.Leader, &followers); err != nil {
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
