require 'mysql2'
require 'ETL'

connection = Mysql2::Client.new host:     'localhost',
                                username: 'root',
                                password: '',
                                database: 'some_database'

# set up the source database
connection.query %[
  CREATE DATABASE IF NOT EXISTS some_database]

connection.query %[
  CREATE TABLE IF NOT EXISTS some_database.some_source_table (
      user_id INT NOT NULL
    , created_at DATETIME NOT NULL
    , amount INT NOT NULL)]

connection.query %[
  TRUNCATE some_database.some_source_table]

connection.query %[
  INSERT INTO some_database.some_source_table (
      user_id
    , created_at
    , amount
  ) VALUES
      (1, UTC_TIMESTAMP, 100)
    , (2, UTC_TIMESTAMP - INTERVAL 3 DAY, 200)
    , (2, UTC_TIMESTAMP - INTERVAL 3 DAY, 400)
    , (2, UTC_TIMESTAMP - INTERVAL 3 DAY, 600)
    , (3, UTC_TIMESTAMP - INTERVAL 3 DAY, 600)
    , (3, UTC_TIMESTAMP - INTERVAL 3 DAY, -100)
    , (3, UTC_TIMESTAMP - INTERVAL 3 DAY, 200)
    , (3, UTC_TIMESTAMP - INTERVAL 4 DAY, 200)]

# set up the ETL
etl = ETL.new(description: "a description of what this ETL does",
              connection:  connection)

# configure ETL
etl.config do |etl|
  etl.ensure_destination do |etl|
    # For most ETLs you may want to ensure that the destination exists, so the
    # #ensure_destination block is ideally suited to fulfill this requirement.
    #
    # By way of example:
    #
    etl.query %[
      CREATE TABLE IF NOT EXISTS some_database.some_destination_table (
          user_id INT UNSIGNED NOT NULL
        , created_date DATE NOT NULL
        , total_amount INT SIGNED NOT NULL
        , message VARCHAR(100) DEFAULT NULL
        , PRIMARY KEY (user_id, created_date)
        , KEY (created_date)
      )]
  end

  etl.before_etl do |etl|
    # All pre-ETL work is performed in this block.
    #
    # This can be thought of as a before-ETL hook that will fire only once. When
    # you are not leveraging the ETL iteration capabilities, the value of this
    # block vs the #etl block is not very clear. We will see how and when to
    # leverage this block effectively when we introduce iteration.
    #
    # As an example, let's say we want to get rid of all entries that have an
    # amount less than zero before moving on to our actual etl:
    #
    etl.query %[DELETE FROM some_database.some_source_table WHERE amount < 0]
  end

  etl.etl do |etl|
    # Here is where the magic happens! This block contains the main ETL
    # operation.
    #
    # For example:
    #
    etl.query %[
      REPLACE INTO some_database.some_destination_table (
          user_id
        , created_date
        , total_amount
      ) SELECT
          sst.user_id
        , DATE(sst.created_at) AS created_date
        , SUM(sst.amount) AS total_amount
      FROM
        some_database.some_source_table sst
      GROUP BY
          sst.user_id
        , DATE(sst.created_at)]
  end

  etl.after_etl do |etl|
    # All post-ETL work is performed in this block.
    #
    # Again, to finish up with an example:
    #
    etl.query %[
      UPDATE some_database.some_destination_table
      SET message = "WOW"
      WHERE total_amount > 100]
  end
end

# ship it
etl.run

puts %[
ETL complete. Now go have a look at some_database.some_destination_table
That was build from some_database.some_source_table using the above ETL configuration.

SELECT * FROM some_database.some_destination_table;]
