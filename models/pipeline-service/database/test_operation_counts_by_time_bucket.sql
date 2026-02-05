{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model calculates the count of test operations grouped by time buckets, including counts for succeeded, warning, and failed operations, along with total counts across all buckets.
*/

WITH
	bucket_counts AS (
		SELECT
			generate_series AS time_from,
			LEAST(
				{{ var('time_to') }},
				generate_series + INTERVAL '{{ var('bucket_size') }} minute' - INTERVAL '1 microseconds'
			) AS time_to,
			COUNT(CASE WHEN operations.operation_status = 'SUCCEEDED' THEN operations.operation_id END) AS succeeded_count,
			COUNT(CASE WHEN operations.operation_status = 'WARNING' THEN operations.operation_id END) AS warning_count,
			COUNT(CASE WHEN operations.operation_status = 'FAILED' THEN operations.operation_id END) AS failed_count
		FROM
			GENERATE_SERIES(
				{{ var('time_from') }},
				{{ var('time_to') }},
				INTERVAL '{{ var('bucket_size') }} minute'
			) AS generate_series
			LEFT JOIN {{ ref('operations') }} ON operations.inserted_at >= generate_series
			AND operations.inserted_at < generate_series + INTERVAL '{{ var('bucket_size') }} minute'
			AND operations.account_id = {{ var('account_id') }}
			AND operations.asset_external_id IN (
				SELECT external_id FROM {{ ref('assets') }}
				WHERE account_id = {{ var('account_id') }}
				AND asset_id = {{ var('asset_id') }}
			)
			AND operations.operation_type = 'TEST'
		GROUP BY
			time_from,
			time_to
	)
	SELECT
		time_from,
		time_to,
		succeeded_count,
		warning_count,
		failed_count,
		(SELECT SUM(succeeded_count) FROM bucket_counts) AS total_succeeded,
		(SELECT SUM(warning_count) FROM bucket_counts) AS total_warning,
		(SELECT SUM(failed_count) FROM bucket_counts) AS total_failed
	FROM
		bucket_counts
	ORDER BY
		time_from;
