package localdb

// TODO: Proposed values

const ddl01 = `
	CREATE TABLE IF NOT EXISTS local_info (
		key TEXT PRIMARY KEY,
		schema_version_init TEXT,
		schema_version_current TEXT
	);

	CREATE TABLE IF NOT EXISTS generations (
		token BIGINT NOT NULL,
		version INT NOT NULL,
		tx BLOB NOT NULL,
		status INT NOT NULL,
		leader INT NOT NULL,
		followers TEXT NOT NULL, -- comma separated values
		PRIMARY KEY (token, version)
	);

	CREATE TABLE IF NOT EXISTS generations_tx (
		tx BLOB PRIMARY KEY,
		status INT
	);
`
