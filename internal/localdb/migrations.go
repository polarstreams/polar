package localdb

const ddl01 = `
	CREATE TABLE IF NOT EXISTS local_info (
		key TEXT PRIMARY KEY,
		schema_version_init TEXT,
		schema_version_current TEXT
	);

	-- Historic record of committed generations
	CREATE TABLE IF NOT EXISTS generations (
		start_token BIGINT NOT NULL,
		end_token BIGINT NOT NULL,
		version INT NOT NULL,
		timestamp BIGINT NOT NULL,
		tx BLOB NOT NULL,
		status INT NOT NULL,
		leader INT NOT NULL,
		followers TEXT NOT NULL, -- comma separated values
		PRIMARY KEY (start_token, version) -- unique constraint for token and version
	);

	-- Contains local information for the status of generations
	-- for rollforward or rollback purposes
	-- Acts like a local source of truth
	CREATE TABLE IF NOT EXISTS transactions (
		tx BLOB PRIMARY KEY,
		origin INT NOT NULL,
		timestamp BIGINT NOT NULL,
		status INT NOT NULL
	);
`
