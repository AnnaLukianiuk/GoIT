WITH monthly_revenue AS (
	SELECT 
			date(date_trunc('month', payment_date)) AS payment_month,
			user_id, 
			game_name,
			sum(revenue_amount_usd) AS total_revenue
	FROM project.games_payments gp
	GROUP BY payment_month, user_id, game_name
),
revenue_months AS (
	SELECT 
			*,
			date(payment_month + INTERVAL '1' month) AS next_month,
			date(payment_month - INTERVAL '1' month) AS previous_month,
			lead(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_payment_month,
			lag(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_payment_month,
			lag(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_month_revenue
	FROM monthly_revenue
),
metrics AS (
	SELECT 
			payment_month,
			user_id, 
			game_name,
			total_revenue,
			CASE 
				WHEN previous_month_revenue IS NULL 
				THEN total_revenue
			END AS new_MRR,
			CASE 
				WHEN previous_payment_month = previous_month AND total_revenue < previous_month_revenue
				THEN total_revenue - previous_month_revenue
			END AS contraction_MRR,
			CASE 
				WHEN previous_payment_month = previous_month AND total_revenue > previous_month_revenue
				THEN total_revenue - previous_month_revenue
			END AS expansion_MRR,
			CASE
				WHEN previous_payment_month != previous_month AND previous_payment_month IS NOT NULL
				THEN total_revenue
			END AS back_from_churn_MRR,
			CASE 
				WHEN next_payment_month IS NULL OR next_payment_month != next_month
				THEN next_month
			END AS churn_month,
			CASE 
				WHEN next_payment_month IS NULL OR next_payment_month != next_month
				THEN total_revenue
			END AS churned_revenue
	FROM revenue_months
)
SELECT
		m.*,
		gpu."language" AS user_language,
		gpu.has_older_device_model,
		gpu.age AS user_age
FROM metrics m
LEFT JOIN project.games_paid_users gpu USING (user_id);
