package main

// BuildContractBatchSQL 은 두 가지 전략을 지원합니다.
// - usePrecomp=true  : 내 프로젝트의 precomp.{tokens_latest, owners_latest}를 이용 (가장 저비용)
// - usePrecomp=false : 공개 테이블을 'slice_contracts' 주소로 반필터링(세미조인)하여 직접 접근(낮은 비용)
func BuildContractBatchSQL(usePrecomp bool, precompDataset string) string {
	if usePrecomp {
		// your-project.precomp.tokens_latest / owners_latest 를 사용
		// precompDataset 예: "your-project.precomp"
		return "" +
			"WITH slice_contracts AS (\n" +
			"  SELECT address, block_timestamp, block_number, bytecode, is_erc20, is_erc721, function_sighashes\n" +
			"  FROM `bigquery-public-data.crypto_ethereum.contracts`\n" +
			"  WHERE block_timestamp >= @start_ts AND block_timestamp < @end_ts\n" +
			"    AND ( @has_ckpt = FALSE\n" +
			"       OR block_timestamp > @last_ts\n" +
			"       OR (block_timestamp = @last_ts AND address > @last_addr) )\n" +
			"), creation_tx AS (\n" +
			"  SELECT t.receipt_contract_address AS address,\n" +
			"         ANY_VALUE(t.from_address) AS creator,\n" +
			"         ANY_VALUE(t.hash) AS creation_tx\n" +
			"  FROM `bigquery-public-data.crypto_ethereum.transactions` t\n" +
			"  JOIN slice_contracts sc ON sc.address = t.receipt_contract_address\n" +
			"  GROUP BY address\n" +
			")\n" +
			"SELECT\n" +
			"  sc.address AS address,\n" +
			"  ctx.creator,\n" +
			"  ctx.creation_tx,\n" +
			"  sc.block_timestamp AS created_at,\n" +
			"  sc.block_number AS created_block,\n" +
			"  TO_HEX(SHA256(IFNULL(sc.bytecode, ''))) AS bytecode_sha256,\n" +
			"  sc.is_erc20,\n" +
			"  sc.is_erc721,\n" +
			"  sc.function_sighashes,\n" +
			"  COALESCE(a.symbol, tl.symbol) AS symbol,\n" +
			"  COALESCE(a.name,   tl.name)   AS name,\n" +
			"  SAFE_CAST(COALESCE(a.decimals, tl.decimals) AS INT64) AS decimals,\n" +
			"  ol.owner\n" +
			"FROM slice_contracts sc\n" +
			"JOIN creation_tx ctx USING(address)\n" +
			"LEFT JOIN `bigquery-public-data.crypto_ethereum.amended_tokens` a ON sc.address = a.address\n" +
			"LEFT JOIN `" + precompDataset + ".tokens_latest`  tl ON sc.address = tl.address\n" +
			"LEFT JOIN `" + precompDataset + ".owners_latest`  ol ON sc.address = ol.contract_address\n" +
			"ORDER BY sc.block_timestamp, sc.address\n" +
			"LIMIT @lim"
	}

	// 공개 테이블 직접 접근(세미조인으로 범위 축소)
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
		"), owners AS (\n" +
		"  SELECT l.address AS contract_address,\n" +
		"         CONCAT('0x', LOWER(REGEXP_EXTRACT(l.data, r'(?i)000000000000000000000000([0-9a-f]{40})$'))) AS owner\n" +
		"  FROM (\n" +
		"    SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.address ORDER BY l.block_timestamp DESC, l.log_index DESC) rn\n" +
		"    FROM `bigquery-public-data.crypto_ethereum.logs` l\n" +
		"    JOIN slice_contracts sc ON sc.address = l.address\n" +
		"    WHERE l.block_timestamp >= @start_ts AND l.block_timestamp < @end_ts\n" +
		"      AND ARRAY_LENGTH(l.topics) > 0\n" +
		"      AND l.topics[SAFE_OFFSET(0)] = '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b0e4b5f8f8fdd8'\n" +
		"  ) WHERE rn = 1\n" +
		"), creation_tx AS (\n" +
		"  SELECT t.receipt_contract_address AS address,\n" +
		"         ANY_VALUE(t.from_address) AS creator,\n" +
		"         ANY_VALUE(t.hash) AS creation_tx\n" +
		"  FROM `bigquery-public-data.crypto_ethereum.transactions` t\n" +
		"  JOIN slice_contracts sc ON sc.address = t.receipt_contract_address\n" +
		"  GROUP BY address\n" +
		")\n" +
		"SELECT\n" +
		"  sc.address AS address,\n" +
		"  ctx.creator,\n" +
		"  ctx.creation_tx,\n" +
		"  sc.block_timestamp AS created_at,\n" +
		"  sc.block_number AS created_block,\n" +
		"  TO_HEX(SHA256(IFNULL(sc.bytecode, ''))) AS bytecode_sha256,\n" +
		"  sc.is_erc20,\n" +
		"  sc.is_erc721,\n" +
		"  sc.function_sighashes,\n" +
		"  COALESCE(a.symbol, lt.symbol) AS symbol,\n" +
		"  COALESCE(a.name,   lt.name)   AS name,\n" +
		"  SAFE_CAST(COALESCE(a.decimals, lt.decimals) AS INT64) AS decimals,\n" +
		"  o.owner\n" +
		"FROM slice_contracts sc\n" +
		"JOIN creation_tx ctx USING(address)\n" +
		"LEFT JOIN `bigquery-public-data.crypto_ethereum.amended_tokens` a ON sc.address = a.address\n" +
		"LEFT JOIN latest_tokens lt ON sc.address = lt.address\n" +
		"LEFT JOIN owners o ON sc.address = o.contract_address\n" +
		"ORDER BY sc.block_timestamp, sc.address\n" +
		"LIMIT @lim"
}
