# ETL

Extract, transform, and load data with ruby!

## Installation

Add this line to your application's Gemfile:

    gem 'ETL'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ETL

## ETL Dependencies

ETL depends on having a database connection object that __must__ respond
to `#query`. The [mysql2](https://github.com/brianmario/mysql2) gem is a good option.
You can also proxy another library using Ruby's `SimpleDelegator` and add a `#query`
method if need be.

The gem comes bundled with a default logger. If you'd like to write your own
just make sure that it implements `#debug` and `#info`. For more information
on what is logged and when, view the [logger details](#logger-details).

### Basic ETL

Assume that we have a database connection represented by `connection`.

To run a basic ETL that is composed of sequential SQL statements, start by
creating a new ETL instance:

```ruby
# setting connection at the class level
ETL.connection = connection

etl = ETL.new(description: "a description of what this ETL does")
```

or

```ruby
# setting connection at the instance level
etl = ETL.new(description: "a description of what this ETL does",
              connection:  connection)
```
which can then be configured:

```ruby
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
          user_id
        , DATE(created_at) AS created_date
        , SUM(amount) AS total_amount
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
```

At this point it is possible to run the ETL instance via:

```ruby
etl.run
```
which executes `#ensure_destination`, `#before_etl`, `#etl`, and `#after_etl` in
that order.

### ETL with iteration

To add in iteration, simply supply `#start`, `#step`, and `#stop` blocks. This
is useful when dealing with large data sets or when executing queries that,
while optimized, are still slow.

Again, to kick things off:

```ruby
etl = ETL.new(description: "a description of what this ETL does",
              connection:  connection)
```

where `connection` is the same as described above.

Next we can configure the ETL:

```ruby
# assuming we have the ETL instance from above
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
```

At this point it is possible to run the ETL instance via:

```ruby
etl.run
```
which executes `#ensure_destination`, `#before_etl`, `#etl`, and `#after_etl` in
that order.

Note that `#etl` executes `#start` and `#stop` once and memoizes the result for
each. It then begins to iterate from what `#start` evaluated to up until what `#stop`
evaluated to by what `#step` evaluates to.

## Examples

There are two examples found in `./examples` that demonstrate the basic ETL and
iteration ETL. Each file uses the [mysql2](https://github.com/brianmario/mysql2)
gem and reads / writes data to localhost using the root user with no password.
Adjust as needed.

## Logger Details

A logger must support two methods: `#info` and `#warn`.

Both methods should accept a single hash argument. The argument will contain:

- `:emitter` => a reference to the ETL instance's `self`
- `:event_type` => a symbol that includes the type of event being logged. You
  can use this value to derive which other data you'll have available

When `:event_type` is equal to `:query_start`, you'll have the following
available in the hash argument:

- `:sql` => the sql that is going to be run

These events are logged at the debug level.

When `:event_type` is equal to `:query_complete`, you'll have the following
available in the hash argument:

- `:sql` => the sql that was run
- `:runtime` => how long the query took to execute

These events are logged at the info level.

Following from this you could implement a simple logger as:

```ruby
class PutsLogger
  def info data
    @data = data
    write!
  end

  def debug data
    @data = data
    write!
  end

private

  def write!
    case (event_type = @data.delete(:event_type))
    when :query_start
      output =  "#{@data[:emitter].description} is about to run\n"
      output += "#{@data[:sql]}\n"
    when :query_complete
      output =  "#{@data[:emitter].description} executed:\n"
      output += "#{@data[:sql]}\n"
      output += "query completed at #{Time.now} and took #{@data[:runtime]}s\n"
    else
      output = "no special logging for #{event_type} event_type yet\n"
    end
    puts output
    @data = nil
  end
end
```

## Contributing

If you would like to contribute code to ETL you can do so through GitHub by
forking the repository and sending a pull request.

When submitting code, please make every effort to follow existing conventions
and style in order to keep the code as readable as possible.

Before your code can be accepted into the project you must also sign the
[Individual Contributor License Agreement (CLA)][1].


 [1]: https://spreadsheets.google.com/spreadsheet/viewform?formkey=dDViT2xzUHAwRkI3X3k5Z0lQM091OGc6MQ&ndplr=1

## License

Copyright 2013 Square Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
