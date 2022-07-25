SELECT
	category,
	film,
	number_of_rentals
FROM
	(SELECT
		ROW_NUMBER() OVER (PARTITION BY category.name ORDER BY COUNT(*) DESC) AS r,
		category.name AS category,
		film.title AS film,
		COUNT(*) AS number_of_rentals
	FROM
		rental
		LEFT JOIN inventory ON rental.inventory_id = inventory.inventory_id
		LEFT JOIN film ON inventory.film_id = film.film_id
		LEFT JOIN film_category ON film.film_id = film_category.film_id
		LEFT JOIN category ON film_category.category_id = category.category_id
	WHERE
		rental.rental_date >= '2005-01-01'
		AND rental.rental_date < '2005-07-01'
	GROUP BY
		category,
		film
	) film_rentals
WHERE
	r <= 10
ORDER BY
	category,
	number_of_rentals DESC,
	film