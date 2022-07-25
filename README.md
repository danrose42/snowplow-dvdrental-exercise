# Technical Test

## Introduction

This technical test makes use of the dvd rentals Postgres database. The entity–relationship model has been provided to you to better understand what is contained in this database.

![ER Model](dvd-rental-db-diagram.png)

The database can be hosted locally using Docker, allowing you to execute queries against it.

We have set a series of tasks for you to complete as outlined below. For each question, please copy the queries you have written to a document (Google Docs, Markdown etc) and share it with us (email, Github etc).

While the data set provided is relatively small, please attempt to write performant queries.

## Set Up

### Prerequisites

- Docker installed on your local machine.
- Cloned the contents of this repository including the docker compose file and dvdrental folder to your machine.

### Hosting the database

Execute:

- `docker compose up`
- In a new terminal window; `docker exec -it pg_container bash`
  - This will give access to the container `pg_container`
- Set the database password inside the container; `set "PGPASSWORD=root"`
- Load the database; `pg_restore -U postgres -d dvdrental dvdrental`

### Connecting to the database

There are many options to connect to the database including:

- psql via CLI
- SQL editor such as DBeaver
- dbt
- pgAdmin

Please use whatever you feel most comfortable with. The connection details can be found in the `docker_compose.yml` file. The host name will likely be `localhost`.

## Questions

 Please attempt to answer as many of the following questions as you feel comfortable with. In the case where you can't quite find the answer, please share your attempt.

1. Find the top 10 most popular movies from rentals in H1 2005, by category.
2. Find the avg. customer value per store by month for rentals in 2005. Please exclude the top & bottom 10% of customers by value from the analysis.
3. Create a table, `film_recommendations`, which provides 10 film recommendations per customer. Future recommendations could be based upon a customer's previous film choices, other customer's choices etc. Please only use SQL to complete this and include all the DDL needed to create the table.
4. Create a table, `customer_lifecycle`, with a primary key of `customer_id`. Please include all the required DDL. This table is designed to provide a holistic view of a customers activity and should include:
    - The revenue generated in the first 30 days of the customer's life-cycle, with day 0 being their first rental date.
    - A value tier based on the first 30 day revenue.
    - The name of the first film they rented.
    - The name of the last film they rented.
    - Last rental date.
    - Avg. time between rentals.
    - Total revenue.
    - The top 3 favorite actors per customer.
    - Any other interesting dimensions or facts you might want to include.
5. Imagine that new rental data is being loaded into the database every hour. Assuming that the data is loaded sequentially, ordered by `rental_date`, re-purpose your logic for the `customer_lifecycle` table to process the new data in an incremental manner to a new table `customer_lifecycle_incremental`.

## Submitting results

Once you have finished please save all your SQL scripts to a Google Doc or a Github repository and share the link with us. Please label which question each script relates to and feel free to add any supporting comments.

Best of luck!
