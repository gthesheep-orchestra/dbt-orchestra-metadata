{{
  config(
    materialized='view'
    , schema='analytics'
    , tags=["analytics"]
  )
}}

/*
  Original source: orchestra-hq/pipeline-service/database/operations.py
  Description: This model retrieves lightweight operations data by joining task runs, pipelines, and operations, applying various filters and transformations to derive operation statuses, durations, and related integration information.
*/

SELECT
		task_runs.task_run_id,
		task_runs.pipeline_id,
		CASE
			WHEN operations.operation_id IS NOT NULL
			THEN operations.operation_status
			ELSE task_runs.status
		END AS operation_status,
		COALESCE(operations.integration, task_runs.integration) AS integration,
		COALESCE(operations.inserted_at, task_runs.completed_at, task_runs.updated_at) AS inserted_at,
		CASE
			WHEN operations.operation_id IS NOT NULL
			THEN operations.operation_duration
			ELSE
				CASE
					WHEN task_runs.completed_at IS NOT NULL
					THEN EXTRACT(EPOCH FROM task_runs.completed_at - task_runs.started_at)
					ELSE NULL
				END
		END AS operation_duration,
		operations.operation_type,
		operations.raw_cost_amount,
		operations.raw_cost_integration,
		operations.raw_cost_label,
		operations.asset_external_id,
		operations.external_id,
		pipelines.product_id
FROM {{ ref('task_runs') }} AS task_runs
INNER JOIN {{ ref('pipelines') }} AS pipelines
	ON pipelines.account_id = {{ account_id }}
	AND task_runs.pipeline_id = pipelines.pipeline_id
LEFT JOIN {{ ref('operations') }} AS operations
	ON operations.account_id = {{ account_id }}
	AND task_runs.pipeline_run_id = operations.pipeline_run_id
	AND task_runs.task_run_id = operations.task_run_id
	AND operations.operation_type <> 'TEST_GROUP'
WHERE task_runs.account_id = {{ account_id }}
AND task_runs.started_at >= {{ time_from }}
AND task_runs.completed_at < {{ time_to }}
AND task_runs.matrix_parent = FALSE
AND pipelines.is_deleted = FALSE
AND ({{ unique_task_ids }} IS NULL OR task_runs.unique_task_id = ANY({{ unique_task_ids }}))
AND ({{ product_ids }} IS NULL OR {{ product_ids_include_none }} = TRUE AND pipelines.product_id IS NULL OR pipelines.product_id = ANY({{ product_ids }}))
AND ({{ pipeline_ids }} IS NULL OR pipelines.pipeline_id = ANY({{ pipeline_ids }}))
AND ({{ statuses }} IS NULL OR COALESCE(operations.operation_status, task_runs.status) = ANY({{ statuses }}))
AND ({{ integrations }} IS NULL OR COALESCE(operations.integration, task_runs.integration) = ANY({{ integrations }}))
AND COALESCE(operations.completed_at, operations.inserted_at, task_runs.completed_at) >= {{ time_from }}
AND COALESCE(operations.completed_at, operations.inserted_at, task_runs.completed_at) <= {{ time_to }};
