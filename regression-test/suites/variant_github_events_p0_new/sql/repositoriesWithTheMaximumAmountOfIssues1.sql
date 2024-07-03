SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */ cast(v["repo"]["name"] as string) as repo_name, count() AS c, count(distinct cast(v["actor"]["login"] as string)) AS u FROM github_events WHERE cast(v["type"] as string) = 'IssuesEvent' AND cast(v["payload"]["action"] as string) = 'opened' GROUP BY repo_name ORDER BY c DESC, repo_name LIMIT 50