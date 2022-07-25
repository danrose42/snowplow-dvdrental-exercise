SELECT
	store,
	yearmonth,
	AVG(total_value) AS avg_customer_value
FROM
	(SELECT
		PERCENT_RANK() OVER (PARTITION BY city.city, TO_CHAR(rental.rental_date, 'YYYY-MM') ORDER BY SUM(payment.amount) DESC) AS pr,
		city.city AS store,
		TO_CHAR(rental.rental_date, 'YYYY-MM') AS yearmonth,
		payment.customer_id,
		SUM(payment.amount) AS total_value
	FROM
		payment
		LEFT JOIN rental ON payment.rental_id = rental.rental_id
		LEFT JOIN customer ON payment.customer_id = customer.customer_id
		LEFT JOIN store ON customer.store_id = store.store_id
		LEFT JOIN address ON store.address_id = address.address_id
		LEFT JOIN city ON address.city_id = city.city_id
	WHERE
		rental.rental_date >= '2005-01-01'
		AND rental.rental_date < '2006-01-01'
	GROUP BY
		store,
		yearmonth,
		payment.customer_id
	) customer_value
WHERE -- Exclude top & bottom 10% of customers by value (by city and month)
	pr > 0.1 AND pr < 0.9
GROUP BY
	store,
	yearmonth
ORDER BY
	store,
	yearmonth