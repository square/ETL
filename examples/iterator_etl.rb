require 'mysql2'
require 'ETL'

connection = Mysql2::Client.new host:     'localhost',
                                username: 'root',
                                password: '',
                                database: 'some_database'

# set up the source database:
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

# configure it
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
    # Now that we are leveraging iteration the #before_etl block becomes
    # more useful as a way to execute an operation once before we begin
    # our iteration.
    #
    # As an example, let's say we want to get rid of all entries that have an
    # amount less than zero before moving on to our actual etl:
    #
    etl.query %[
      DELETE FROM some_database.some_source_table
      WHERE amount < 0]
  end

  etl.start do |etl|
    # This defines where the ETL should start. This can be a flat number
    # or date, or even SQL / other code can be executed to produce a starting
    # value.
    #
    # Usually, this is the last known entry for the destination table with
    # some sensible default if the destination does not yet contain data.
    #
    # As an example:
    #
    # Note that we cast the default date as a DATE. If we don't, it will be
    # treated as a string and our iterator will fail under the hood when testing
    # if it is complete.
    res = etl.query %[
      SELECT COALESCE(MAX(created_date), DATE('2010-01-01')) AS the_max
      FROM some_database.some_destination_table]

    res.to_a.first['the_max']
  end

  etl.step do |etl|
    # The step block defines the size of the iteration block. To iterate by
    # ten records, the step block should be set to return 10.
    #
    # As an alternative example, to set the iteration to go 10,000 units
    # at a time, the following value should be provided:
    #
    #   10_000 (Note: an underscore is used for readability)
    #
    # As an example, to iterate 7 days at a time:
    #
    7
  end

  etl.stop do |etl|
    # The stop block defines when the iteration should halt.
    # Again, this can be a flat value or code. Either way, one value *must* be
    # returned.
    #
    # As a flat value:
    #
    #   1_000_000
    #
    # Or a date value:
    #
    #   Time.now.to_date
    #
    # Or as a code example:
    #
    res = etl.query %[
      SELECT DATE(MAX(created_at)) AS the_max
      FROM some_database.some_source_table]

    res.to_a.first['the_max']
  end

  etl.etl do |etl, lbound, ubound|
    # The etl block is the main part of the framework. Note: there are
    # two extra args with the iterator this time around: "lbound" and "ubound"
    #
    # "lbound" is the lower bound of the current iteration. When iterating
    # from 0 to 10 and stepping by 2, the lbound would equal 2 on the
    # second iteration.
    #
    # "ubound" is the upper bound of the current iteration. In continuing with the
    # example above, when iterating from 0 to 10 and stepping by 2, the ubound would
    # equal 4 on the second iteration.
    #
    # These args can be used to "window" SQL queries or other code operations.
    #
    # As a first example, to iterate over a set of ids:
    #
    #   etl.query %[
    #     REPLACE INTO some_database.some_destination_table (
    #         created_date
    #       , user_id
    #       , total_amount
    #     ) SELECT
    #         DATE(sst.created_at) AS created_date
    #       , sst.user_id
    #       , SUM(sst.amount) AS total_amount
    #     FROM
    #       some_database.some_source_table sst
    #     WHERE
    #       sst.user_id > #{lbound} AND sst.user_id <= #{ubound}
    #     GROUP BY
    #         DATE(sst.created_at)
    #       , sst.user_id]
    #
    # To "window" a SQL query using dates:
    #
    etl.query %[
      REPLACE INTO some_database.some_destination_table (
          created_date
        , user_id
        , total_amount
      ) SELECT
          DATE(sst.created_at) AS created_date
        , sst.user_id
        , SUM(sst.amount) AS total_amount
      FROM
        some_database.some_source_table sst
      WHERE
        -- Note the usage of quotes surrounding the lbound and ubound vars.
        -- This is is required when dealing with dates / datetimes
        sst.created_at >= '#{lbound}' AND sst.created_at < '#{ubound}'
      GROUP BY
          DATE(sst.created_at)
        , sst.user_id]

    # Note that there is no sql sanitization here so there is *potential* for SQL
    # injection. That being said you'll likely be using this gem in an internal
    # tool so hopefully your co-workers are not looking to sabotage your ETL
    # pipeline. Just be aware of this and handle it as you see fit.
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

etl.run

puts %[
ETL complete. Now go have a look at some_database.some_destination_table
That was build from some_database.some_source_table using the above ETL configuration.

SELECT * FROM some_database.some_destination_table;]
