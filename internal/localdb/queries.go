package localdb

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
