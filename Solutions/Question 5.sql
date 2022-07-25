-- Solution assumes this script would be run automatically each time a data upload finishes. Have opted to
-- use the most recent value of 'last_rental_datetime' from existing 'customer_lifecycle_incremental' table,
-- to determine which rental entries are new. Have also opted to delete and reinsert the entire row for each
-- existing customer with a new rental entry, to simplify logic (all fields would need recalculating anyway
-- except for 'first_rental', 'first_30_days_revenue' and 'customer_value_tier' - and the latter pair would
-- still need recalculating for the first 30 days in the case of an entirely new customer, adding further
-- complexity). No recalculating fields or delete/insert rows are done for customers without a new record.


-- DROP TABLE IF EXISTS customer_lifecycle_incremental;

-- CREATE TABLE IF NOT EXISTS customer_lifecycle_incremental (
-- 	customer_id INT PRIMARY KEY,
-- 	first_30_days_revenue FLOAT,
-- 	customer_value_tier CHAR(1),
-- 	first_rental TEXT,
-- 	last_rental TEXT,
-- 	last_rental_datetime TIMESTAMP,
-- 	avg_days_between_rentals INT,
-- 	lifetime_revenue FLOAT,
-- 	favourite_actors TEXT,
-- 	favourite_categories TEXT
-- );

WITH previous_update AS ( -- Get most recent 'last_rental_datetime' value from table
	SELECT
		MAX(last_rental_datetime) AS last_update_time
	FROM
		customer_lifecycle_incremental
),
new_rentals AS ( -- Get a list of customers where there is a new rental record (since last update)
	SELECT DISTINCT
		customer_id
	FROM
		rental
		LEFT JOIN previous_update ON TRUE
	WHERE
		rental_date > previous_update.last_update_time
)
DELETE FROM customer_lifecycle_incremental
WHERE customer_id IN ( -- Delete rows in table for existing customer with a new rental record
	SELECT DISTINCT
		customer_id
	FROM
		rental
		LEFT JOIN previous_update ON TRUE
	WHERE
		rental_date > previous_update.last_update_time
);

-- Insert/reinsert rows in table for customers where there is a new rental record
INSERT INTO customer_lifecycle_incremental (
	customer_id,
	first_30_days_revenue,
	customer_value_tier,
	first_rental,
	last_rental,
	last_rental_datetime, -- Swapped from Date to Timestamp 
	avg_days_between_rentals,
	lifetime_revenue,
	favourite_actors,
	favourite_categories
)
SELECT * FROM (
	
	WITH previous_update AS (
		SELECT
			MAX(last_rental_datetime) AS last_update_time
		FROM
			customer_lifecycle_incremental
	),
	new_rentals AS (
		SELECT DISTINCT
			customer_id
		FROM
			rental
			LEFT JOIN previous_update ON TRUE
		WHERE
			rental_date > previous_update.last_update_time
	),
	key_dates AS (
		SELECT
			customer_id,
			MIN(DATE(rental_date)) AS first_rental_date,
			MAX(DATE(rental_date)) AS last_rental_date,
			MIN(rental_date) AS first_rental_datetime,
			MAX(rental_date) AS last_rental_datetime
		FROM
			rental
		WHERE -- Only calculate fields for customers where there is a new rental record
			customer_id IN (SELECT customer_id FROM new_rentals)
		GROUP BY
			customer_id
	),
	rental_detail AS (
		SELECT
			rental.customer_id,
			DATE(rental.rental_date) AS rental_date,
			rental.rental_date AS rental_datetime,
			payment.amount AS revenue,
			film.title AS film_name
		FROM
			rental
			LEFT JOIN payment ON rental.rental_id = payment.rental_id
			LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
			LEFT JOIN film ON inventory.film_id = film.film_id
		WHERE -- Only calculate fields for customers where there is a new rental record
			rental.customer_id IN (SELECT customer_id FROM new_rentals)
	),
	revenue_30_days AS (
		SELECT
			customer_id,
			revenue,
			CASE
				WHEN pr >= 0.9 THEN 'A'
				WHEN pr >= 0.8 THEN 'B'
				WHEN pr >= 0.6 THEN 'C'
				WHEN pr >= 0.4 THEN 'D'
				WHEN pr >= 0.2 THEN 'E'
				ELSE 'F'
			END AS customer_value_tier
		FROM (
			SELECT
				customer_id,
				revenue,
				PERCENT_RANK() OVER (PARTITION BY CASE WHEN revenue IS NULL THEN 0 ELSE 1 END
									 ORDER BY revenue) AS pr
			FROM (
				SELECT 
					rental_detail.customer_id,
					SUM(rental_detail.revenue) AS revenue
				FROM
					rental_detail
					LEFT JOIN key_dates ON rental_detail.customer_id = key_dates.customer_id
				WHERE
					rental_detail.rental_date < key_dates.first_rental_date+30
				GROUP BY
					rental_detail.customer_id
			) customers_unranked
		) customers_ranked
	),
	first_rental AS (
		SELECT
			rental_detail.customer_id,
			ARRAY_AGG(film_name) AS film
		FROM
			rental_detail
			LEFT JOIN key_dates ON rental_detail.customer_id = key_dates.customer_id
		WHERE
			rental_detail.rental_datetime = key_dates.first_rental_datetime
		GROUP BY
			rental_detail.customer_id
	),
	last_rental AS (
		SELECT
			rental_detail.customer_id,
			ARRAY_AGG(film_name) AS film
		FROM
			rental_detail
			LEFT JOIN key_dates ON rental_detail.customer_id = key_dates.customer_id
		WHERE
			rental_detail.rental_datetime = key_dates.last_rental_datetime
		GROUP BY
			rental_detail.customer_id
	),
	time_between_rentals AS (
		WITH ordered_rentals AS (
			SELECT 
				customer_id,
				rental_datetime,
				ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY rental_datetime) AS r
			FROM (
				SELECT DISTINCT
					customer_id,
					rental_datetime
				FROM
					rental_detail
			) unique_rentals
		)
		SELECT
			or1.customer_id,
			EXTRACT(epoch FROM AVG(or2.rental_datetime - or1.rental_datetime)) AS avg_time_seconds
		FROM
			ordered_rentals AS or1
			LEFT JOIN ordered_rentals AS or2 ON (or1.customer_id = or2.customer_id AND or1.r = or2.r-1)
		WHERE
			or2.customer_id IS NOT NULL
		GROUP BY or1.customer_id
	),
	total_revenue AS (
		SELECT 
			rental_detail.customer_id,
			SUM(rental_detail.revenue) AS revenue
		FROM
			rental_detail
		GROUP BY
			rental_detail.customer_id
	),
	favourite_actors AS (
		SELECT
			customer_id,
			ARRAY_AGG(actor_name) AS customer_favorite_actors
		FROM (
			SELECT
				rental.customer_id,
				actor.first_name || ' ' || actor.last_name AS actor_name,
				ROW_NUMBER() OVER (PARTITION BY rental.customer_id ORDER BY COUNT(*) DESC) AS r
			FROM
				rental
				LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
				LEFT JOIN film ON inventory.film_id = film.film_id
				LEFT JOIN film_actor ON film.film_id = film_actor.film_id
				LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
			WHERE -- Only calculate fields for customers where there is a new rental record
				rental.customer_id IN (SELECT customer_id FROM new_rentals)
			GROUP BY
				rental.customer_id,
				actor.actor_id
		) actors
		WHERE
			r < 4
		GROUP BY
			customer_id
	),
	favourite_categories AS (
		SELECT
			customer_id,
			ARRAY_AGG(category_name) AS customer_favorite_categories
		FROM (
			SELECT
				rental.customer_id,
				category.name AS category_name,
				ROW_NUMBER() OVER (PARTITION BY rental.customer_id ORDER BY COUNT(*) DESC) AS r
			FROM
				rental
				LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
				LEFT JOIN film ON inventory.film_id = film.film_id
				LEFT JOIN film_category ON film.film_id = film_category.film_id
				LEFT JOIN category ON film_category.category_id = category.category_id
			WHERE -- Only calculate fields for customers where there is a new rental record
				rental.customer_id IN (SELECT customer_id FROM new_rentals)
			GROUP BY
				rental.customer_id,
				category.category_id
		) categories
		WHERE
			r < 4
		GROUP BY
			customer_id
	)

	SELECT
		customer.customer_id,
		revenue_30_days.revenue AS first_30_days_revenue,
		revenue_30_days.customer_value_tier,
		first_rental.film AS first_rental,
		last_rental.film AS last_rental,
		key_dates.last_rental_datetime, -- Swapped from Date to Timestamp
		ROUND(time_between_rentals.avg_time_seconds/3600/24) AS avg_days_between_rentals,
		total_revenue.revenue AS lifetime_revenue,
		favourite_actors.customer_favorite_actors AS favourite_actors,
		favourite_categories.customer_favorite_categories AS favourite_categories
	FROM
		customer
		LEFT JOIN revenue_30_days ON customer.customer_id = revenue_30_days.customer_id
		LEFT JOIN first_rental ON customer.customer_id = first_rental.customer_id
		LEFT JOIN last_rental ON customer.customer_id = last_rental.customer_id
		LEFT JOIN key_dates ON customer.customer_id = key_dates.customer_id
		LEFT JOIN time_between_rentals ON customer.customer_id = time_between_rentals.customer_id
		LEFT JOIN total_revenue ON customer.customer_id = total_revenue.customer_id
		LEFT JOIN favourite_actors ON customer.customer_id = favourite_actors.customer_id
		LEFT JOIN favourite_categories ON customer.customer_id = favourite_categories.customer_id
	WHERE -- Only calculate fields for customers where there is a new rental record
		customer.customer_id IN (SELECT customer_id FROM new_rentals)
	
) customer_lifecycle_data