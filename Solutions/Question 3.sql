-- DROP TABLE IF EXISTS film_recommendations;

CREATE TABLE IF NOT EXISTS film_recommendations AS

SELECT * FROM (
	WITH all_films AS ( -- All film/customer combinations (including those already watched)
		SELECT
			film.film_id,
			film.title AS film,
			film_category.category_id,
			category.name AS category,
			customer.customer_id,
			customer.email AS customer
		FROM
			film
			LEFT JOIN film_category ON film.film_id = film_category.film_id
			LEFT JOIN category ON film_category.category_id = category.category_id
			FULL JOIN customer ON TRUE
	),
	customer_rentals AS ( -- Films already watched for each customer
		SELECT
			inventory.film_id,
			rental.customer_id
		FROM
			inventory
			LEFT JOIN rental ON inventory.inventory_id = rental.inventory_id
	),
	customer_ranked_categories AS ( -- Film categories ranked for each customer based on previous rentals
		SELECT
			rental.customer_id,
			film_category.category_id,
			RANK() OVER (PARTITION BY rental.customer_id ORDER BY COUNT(*) DESC) AS rnk
		FROM
			rental
			LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
			LEFT JOIN film ON inventory.film_id = film.film_id
			LEFT JOIN film_category ON film.film_id = film_category.film_id
		GROUP BY
			rental.customer_id,
			film_category.category_id
	),
	total_rentals AS ( -- Total rentals for each film (all time)
		SELECT
			film.film_id,
			COUNT(*) AS film_rentals
		FROM
			rental
			LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
			LEFT JOIN film ON inventory.film_id = film.film_id
		GROUP BY
			film.film_id
	),
	customer_unwatched_films AS ( -- Unwatched films for each customer
		SELECT
			all_films.customer_id,
			all_films.customer,
			all_films.film_id,
			all_films.film,
			all_films.category_id,
			all_films.category
		FROM
			all_films
			LEFT JOIN customer_rentals ON (all_films.film_id = customer_rentals.film_id
										   AND all_films.customer_id = customer_rentals.customer_id)
		WHERE
			customer_rentals.film_id IS NULL
	),
	customer_weighted_films AS ( -- Film reccomendations weighted for each customer using their category preference
		SELECT					 -- eg. Total number of rentals/1 for customer's top category, /2 for second etc.
			customer_unwatched_films.customer_id,
			customer_unwatched_films.customer,
			customer_unwatched_films.film_id,
			customer_unwatched_films.film,
			customer_unwatched_films.category_id,
			customer_unwatched_films.category,
			total_rentals.film_rentals/customer_ranked_categories.rnk AS customer_film_weighting,
			ROW_NUMBER() OVER (PARTITION BY customer_unwatched_films.customer_id
							   ORDER BY total_rentals.film_rentals/customer_ranked_categories.rnk DESC) AS r
		FROM
			customer_unwatched_films
			LEFT JOIN customer_ranked_categories ON (customer_unwatched_films.customer_id = customer_ranked_categories.customer_id
													 AND customer_unwatched_films.category_id = customer_ranked_categories.category_id)
			LEFT JOIN total_rentals ON customer_unwatched_films.film_id = total_rentals.film_id
		WHERE -- Ignore films from categories customer has never rented, and ignore films with zero total rentals
			customer_ranked_categories.rnk IS NOT NULL
			AND total_rentals.film_rentals IS NOT NULL
	)

	SELECT -- Top 10 film reccomendations for each customer
		customer_id,
		customer,
		r AS reccomendation,
		film,
		category
	FROM
		customer_weighted_films
	WHERE
		r <= 10
) film_recommendations_data