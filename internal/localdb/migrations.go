package localdb

var migrationQueries = []string{migration1, migration2}

const migration1 = `
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
		tx_leader INT NOT NULL,
		status INT NOT NULL,
		leader INT NOT NULL,
		followers TEXT NOT NULL, -- comma separated values
		parents TEXT NOT NULL, -- json of []GenId, defaulting to empty array
		PRIMARY KEY (start_token, version)
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

	CREATE TABLE IF NOT EXISTS offsets (
		group_name TEXT NOT NULL,
		topic TEXT NOT NULL,
		token BIGINT NOT NULL,
		range_index INT NOT NULL,
		version INT NOT NULL,
		offset INT NOT NULL,
		source TEXT NOT NULL, -- json of GenId
		PRIMARY KEY (group_name, topic, token, range_index)
	);
`

const migration2 = `
	ALTER TABLE generations ADD cluster_size int NOT NULL DEFAULT 3;
`
