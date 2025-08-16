package main

// BuildContractBatchSQL: ownerlog(이력)까지 포함하도록 확장
// - usePrecomp=true  : precomp.{tokens_latest, owners_latest} + logs로 ownerlog 구성
// - usePrecomp=false : 공개 테이블만으로 tokens 최신/ownerlog/owner 구성
func BuildContractBatchSQL(usePrecomp bool, precompDataset string) string {
	if usePrecomp {
		// precomp 우선 사용 + logs로 ownerlog 구성
		return "" +
			"WITH slice_contracts AS (\n" +
			"  SELECT address, block_timestamp, block_number, bytecode, is_erc20, is_erc721, function_sighashes\n" +
			"  FROM `bigquery-public-data.crypto_ethereum.contracts`\n" +
			"  WHERE block_timestamp >= @start_ts AND block_timestamp < @end_ts\n" +
			"    AND ( @has_ckpt = FALSE\n" +
			"       OR block_timestamp > @last_ts\n" +
			"       OR (block_timestamp = @last_ts AND address > @last_addr) )\n" +
			"), creation_tx AS (\n" +
			"  SELECT receipt_contract_address AS address, from_address AS creator, hash AS creation_tx\n" +
			"  FROM (\n" +
			"    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.receipt_contract_address ORDER BY t.block_number ASC, t.transaction_index ASC) rn\n" +
			"    FROM `bigquery-public-data.crypto_ethereum.transactions` t\n" +
			"    JOIN slice_contracts sc ON sc.address = t.receipt_contract_address\n" +
			"  ) WHERE rn = 1\n" +
			"), owner_events AS (\n" +
			"  SELECT\n" +
			"    l.address AS contract_address,\n" +
			"    l.block_timestamp, l.block_number, l.log_index, l.transaction_hash,\n" +
			"    LOWER(CONCAT('0x', SUBSTR(l.topics[OFFSET(1)], 27, 40))) AS from_owner_topic,\n" +
			"    LOWER(CONCAT('0x', SUBSTR(l.topics[OFFSET(2)], 27, 40))) AS to_owner_topic,\n" +
			"    LOWER(CONCAT('0x', REGEXP_EXTRACT(l.data, r'(?i)000000000000000000000000([0-9a-f]{40})$'))) AS to_owner_data\n" +
			"  FROM `bigquery-public-data.crypto_ethereum.logs` l\n" +
			"  JOIN slice_contracts sc ON sc.address = l.address\n" +
			"  WHERE l.block_timestamp < @end_ts\n" +
			"    AND ARRAY_LENGTH(l.topics) >= 1\n" +
			"    AND l.topics[OFFSET(0)] = '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b0e4b5f8f8fdd8'\n" +
			"), owner_agg AS (\n" +
			"  SELECT\n" +
			"    contract_address,\n" +
			"    ARRAY_AGG(\n" +
			"      FORMAT('%s|%s|%s|%s|%d|%d',\n" +
			"             FORMAT_TIMESTAMP('%FT%TZ', block_timestamp),\n" +
			"             COALESCE(from_owner_topic, '0x0000000000000000000000000000000000000000'),\n" +
			"             COALESCE(to_owner_topic, to_owner_data, '0x0000000000000000000000000000000000000000'),\n" +
			"             LOWER(transaction_hash), block_number, log_index)\n" +
			"      ORDER BY block_timestamp, log_index\n" +
			"    ) AS ownerlog,\n" +
			"    COALESCE(\n" +
			"      ARRAY_REVERSE(ARRAY_AGG(COALESCE(to_owner_topic, to_owner_data) ORDER BY block_timestamp, log_index))[OFFSET(0)],\n" +
			"      NULL\n" +
			"    ) AS owner_from_logs\n" +
			"  FROM owner_events\n" +
			"  GROUP BY contract_address\n" +
			")\n" +
			"SELECT\n" +
			"  LOWER(sc.address) AS address,\n" +
			"  LOWER(ctx.creator) AS creator,\n" +
			"  LOWER(ctx.creation_tx) AS creation_tx,\n" +
			"  sc.block_timestamp AS created_at,\n" +
			"  sc.block_number AS created_block,\n" +
			"  TO_HEX(SHA256(IFNULL(sc.bytecode, ''))) AS bytecode_sha256,\n" +
			"  sc.is_erc20,\n" +
			"  sc.is_erc721,\n" +
			"  sc.function_sighashes,\n" +
			"  COALESCE(a.symbol, tl.symbol) AS symbol,\n" +
			"  COALESCE(a.name,   tl.name)   AS name,\n" +
			"  SAFE_CAST(COALESCE(a.decimals, tl.decimals) AS INT64) AS decimals,\n" +
			"  LOWER(COALESCE(ol.owner, oa.owner_from_logs)) AS owner,\n" +
			"  oa.ownerlog AS ownerlog\n" +
			"FROM slice_contracts sc\n" +
			"JOIN creation_tx ctx USING(address)\n" +
			"LEFT JOIN `bigquery-public-data.crypto_ethereum.amended_tokens` a ON sc.address = a.address\n" +
			"LEFT JOIN `" + precompDataset + ".tokens_latest`  tl ON sc.address = tl.address\n" +
			"LEFT JOIN `" + precompDataset + ".owners_latest`  ol ON sc.address = ol.contract_address\n" +
			"LEFT JOIN owner_agg oa ON sc.address = oa.contract_address\n" +
			"ORDER BY sc.block_timestamp, sc.address\n" +
			"LIMIT @lim"
	}

	// 공개 테이블만으로 계산
	return "" +
		"WITH slice_contracts AS (\n" +
		"  SELECT address, block_timestamp, block_number, bytecode, is_erc20, is_erc721, function_sighashes\n" +
		"  FROM `bigquery-public-data.crypto_ethereum.contracts`\n" +
		"  WHERE block_timestamp >= @start_ts AND block_timestamp < @end_ts\n" +
		"    AND ( @has_ckpt = FALSE\n" +
		"       OR block_timestamp > @last_ts\n" +
		"       OR (block_timestamp = @last_ts AND address > @last_addr) )\n" +
		"), latest_tokens AS (\n" +
		"  SELECT address, symbol, name, decimals FROM (\n" +
		"    SELECT t.address, t.symbol, t.name, t.decimals,\n" +
		"           ROW_NUMBER() OVER (PARTITION BY t.address ORDER BY t.block_timestamp DESC) rn\n" +
		"    FROM `bigquery-public-data.crypto_ethereum.tokens` t\n" +
		"    JOIN slice_contracts sc USING(address)\n" +
		"  ) WHERE rn = 1\n" +
		"), owner_events AS (\n" +
		"  SELECT\n" +
		"    l.address AS contract_address,\n" +
		"    l.block_timestamp, l.block_number, l.log_index, l.transaction_hash,\n" +
		"    LOWER(CONCAT('0x', SUBSTR(l.topics[OFFSET(1)], 27, 40))) AS from_owner_topic,\n" +
		"    LOWER(CONCAT('0x', SUBSTR(l.topics[OFFSET(2)], 27, 40))) AS to_owner_topic,\n" +
		"    LOWER(CONCAT('0x', REGEXP_EXTRACT(l.data, r'(?i)000000000000000000000000([0-9a-f]{40})$'))) AS to_owner_data\n" +
		"  FROM `bigquery-public-data.crypto_ethereum.logs` l\n" +
		"  JOIN slice_contracts sc ON sc.address = l.address\n" +
		"  WHERE l.block_timestamp < @end_ts\n" +
		"    AND ARRAY_LENGTH(l.topics) >= 1\n" +
		"    AND l.topics[OFFSET(0)] = '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b0e4b5f8f8fdd8'\n" +
		"), owner_agg AS (\n" +
		"  SELECT\n" +
		"    contract_address,\n" +
		"    ARRAY_AGG(\n" +
		"      FORMAT('%s|%s|%s|%s|%d|%d',\n" +
		"             FORMAT_TIMESTAMP('%FT%TZ', block_timestamp),\n" +
		"             COALESCE(from_owner_topic, '0x0000000000000000000000000000000000000000'),\n" +
		"             COALESCE(to_owner_topic, to_owner_data, '0x0000000000000000000000000000000000000000'),\n" +
		"             LOWER(transaction_hash), block_number, log_index)\n" +
		"      ORDER BY block_timestamp, log_index\n" +
		"    ) AS ownerlog,\n" +
		"    COALESCE(\n" +
		"      ARRAY_REVERSE(ARRAY_AGG(COALESCE(to_owner_topic, to_owner_data) ORDER BY block_timestamp, log_index))[OFFSET(0)],\n" +
		"      NULL\n" +
		"    ) AS owner\n" +
		"  FROM owner_events\n" +
		"  GROUP BY contract_address\n" +
		"), creation_tx AS (\n" +
		"  SELECT receipt_contract_address AS address, from_address AS creator, hash AS creation_tx\n" +
		"  FROM (\n" +
		"    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.receipt_contract_address ORDER BY t.block_number ASC, t.transaction_index ASC) rn\n" +
		"    FROM `bigquery-public-data.crypto_ethereum.transactions` t\n" +
		"    JOIN slice_contracts sc ON sc.address = t.receipt_contract_address\n" +
		"  ) WHERE rn = 1\n" +
		")\n" +
		"SELECT\n" +
		"  LOWER(sc.address) AS address,\n" +
		"  LOWER(ctx.creator) AS creator,\n" +
		"  LOWER(ctx.creation_tx) AS creation_tx,\n" +
		"  sc.block_timestamp AS created_at,\n" +
		"  sc.block_number AS created_block,\n" +
		"  TO_HEX(SHA256(IFNULL(sc.bytecode, ''))) AS bytecode_sha256,\n" +
		"  sc.is_erc20,\n" +
		"  sc.is_erc721,\n" +
		"  sc.function_sighashes,\n" +
		"  COALESCE(a.symbol, lt.symbol) AS symbol,\n" +
		"  COALESCE(a.name,   lt.name)   AS name,\n" +
		"  SAFE_CAST(COALESCE(a.decimals, lt.decimals) AS INT64) AS decimals,\n" +
		"  LOWER(o.owner) AS owner,\n" +
		"  o.ownerlog AS ownerlog\n" +
		"FROM slice_contracts sc\n" +
		"JOIN creation_tx ctx USING(address)\n" +
		"LEFT JOIN `bigquery-public-data.crypto_ethereum.amended_tokens` a ON sc.address = a.address\n" +
		"LEFT JOIN latest_tokens lt ON sc.address = lt.address\n" +
		"LEFT JOIN owner_agg o ON sc.address = o.contract_address\n" +
		"ORDER BY sc.block_timestamp, sc.address\n" +
		"LIMIT @lim"
}
