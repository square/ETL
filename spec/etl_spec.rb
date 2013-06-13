require 'mysql2'
require 'active_support/time'
require 'etl'

def test_connection
  Mysql2::Client.new host: 'localhost', username: 'root', database: 'etl_test'
end

def reset_test_env connection, &block
  connection.query %[DROP DATABASE IF EXISTS etl_test]
  connection.query %[CREATE DATABASE etl_test]
  connection.query %[USE etl_test]

  if block_given?
    yield connection
  else
    connection.query %[
      CREATE TABLE etl_source (
          id INT NOT NULL
        , name VARCHAR(10)
        , amount INT(11) DEFAULT 0
        , PRIMARY KEY (id))]

    connection.query %[
      INSERT INTO etl_test.etl_source (id, name, amount)
      VALUES
        (1, 'Jeff', 100),
        (2, 'Ryan',  50),
        (3, 'Jack',  75),
        (4, 'Jeff',  10),
        (5, 'Jack',  45),
        (6, 'Nick', -90),
        (7, 'Nick',  90)
    ]
  end
end

describe ETL do
  let(:logger) { nil }

  describe "deprecations" do
    let(:etl) { described_class.new }

    context "#ensure_destination" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.ensure_destination {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #ensure_destination will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.ensure_destination('some arg') {}
      end
    end

    context "#before_etl" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.before_etl {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #before_etl will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.before_etl('some arg') {}
      end
    end

    context "#start" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.start {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #start will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.start('some arg') {}
      end
    end

    context "#step" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.step {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #step will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.step('some arg') {}
      end
    end

    context "#stop" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.stop {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #stop will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.stop('some arg') {}
      end
    end

    context "#etl" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.etl {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #etl will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.etl('some arg') {}
      end
    end

    context "#after_etl" do
      it "does not warn when no args are passed" do
        etl.should_receive(:warn).never
        etl.after_etl {}
      end

      it "warns when args are passed that this is deprecated" do
        etl.should_receive(:warn).with("DEPRECATED: passing arguments to #after_etl will be removed in an upcoming release and will raise an exception. Please remove this from your code.")
        etl.after_etl('some arg') {}
      end
    end
  end

  describe ".connection=" do
    let(:class_level_connection) { stub('class_level_connection') }

    it "sets the #connection for all instances" do
      ETL.connection = class_level_connection
      etl = ETL.new
      expect(etl.connection).to eq class_level_connection
    end

    it "allows instance-level overrides" do
      instance_level_connection = stub('instance_level_connection')
      ETL.connection = class_level_connection
      etl_with_connection_override = ETL.new connection: instance_level_connection
      etl = ETL.new
      expect(etl.connection).to eq class_level_connection
      expect(etl_with_connection_override.connection).to eq instance_level_connection
    end
  end

  describe "#logger=" do
    let(:etl) { described_class.new connection: stub }

    it 'assigns' do
      logger = stub
      etl.logger = logger
      etl.logger.should == logger
    end
  end

  describe '#max_for' do
    let(:connection) { test_connection }
    let(:etl)        { described_class.new connection: connection, logger: logger }

    before do
      client = Mysql2::Client.new host: 'localhost', username: 'root'
      client.query %[DROP DATABASE IF EXISTS etl_test]
      client.query %[CREATE DATABASE etl_test]
      client.query %[USE etl_test]
      client.query %[
        CREATE TABLE IF NOT EXISTS etl_source (
            id INT(11) NOT NULL AUTO_INCREMENT
          , name VARCHAR(10)
          , amount INT(11) DEFAULT 0
          , the_date DATE DEFAULT NULL
          , the_null_date DATE DEFAULT NULL
          , the_time_at DATETIME DEFAULT NULL
          , the_null_time_at DATETIME DEFAULT NULL
          , PRIMARY KEY (id))]

      client.query %[
        INSERT INTO etl_source (
            name
          , amount
          , the_date
          , the_null_date
          , the_time_at
          , the_null_time_at
        ) VALUES
            ('Jeff', 100, '2012-01-02', NULL, '2012-01-02 00:00:01', NULL)
          , ('Ryan',  50, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)
          , ('Jack',  75, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)
          , ('Jeff',  10, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)
          , ('Jack',  45, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)
          , ('Nick', -90, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)
          , ('Nick',  90, '2012-01-01', NULL, '2012-01-01 00:00:00', NULL)]

      client.close
    end

    after { connection.close }

    it "finds the max for dates" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_date).should == Date.parse('2012-01-02')
    end

    it "defaults to the beginning of time date when a max date cannot be found" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_null_date).should == Date.parse('1970-01-01')
    end

    it "defaults to the specified default floor when a max date cannot be found" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_null_date,
                  default_floor: '2011-01-01').should == Date.parse('2011-01-01')
    end

    it "finds the max for datetimes" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_time_at).should == Date.parse('2012-01-02')
    end

    it "defaults to the beginning of time when a max datetime cannot be found" do
      etl.max_for(database: :etl_test,
                  table:    :etl_source,
                  column:   :the_null_time_at).should == Date.parse('1970-01-01 00:00:00')
    end

    it "defaults to the specified default floor when a max datetime cannot be found" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :the_null_time_at,
                  default_floor: '2011-01-01 00:00:00').should == Date.parse('2011-01-01 00:00:00')
    end

    it "raises an error if a non-standard column is supplied with no default floor" do
      expect {
        etl.max_for database: :etl_test,
                    table:    :etl_source,
                    column:   :amount
      }.to raise_exception
    end

    it "finds the max for a non-standard column, using the default floor" do
      etl.max_for(database:      :etl_test,
                  table:         :etl_source,
                  column:        :amount,
                  default_floor: 0).should == 100
    end
  end

  describe '#run' do
    let(:connection) { test_connection }
    let(:etl)        { described_class.new connection: connection, logger: logger }

    before do
      client = Mysql2::Client.new host: 'localhost', username: 'root'
      client.query %[DROP DATABASE IF EXISTS etl_test]
      client.query %[CREATE DATABASE etl_test]
      client.query %[USE etl_test]
      client.query %[
        CREATE TABLE IF NOT EXISTS etl_source (
            id INT(11) NOT NULL AUTO_INCREMENT
          , name VARCHAR(10)
          , amount INT(11) DEFAULT 0
          , PRIMARY KEY (id))]

      client.query %[
        INSERT INTO etl_source (name, amount)
        VALUES
          ('Jeff',  100),
          ('Ryan',  50),
          ('Jack',  75),
          ('Jeff',  10),
          ('Jack',  45),
          ('Nick', -90),
          ('Nick',  90)]

      client.close
    end

    it "executes the specified sql in the appropriate order" do
      etl.ensure_destination do |etl|
        etl.query %[
          CREATE TABLE IF NOT EXISTS etl_destination (
            name VARCHAR(10)
          , total_amount INT(11) DEFAULT 0
          , PRIMARY KEY (name))]
      end

      etl.before_etl do |etl|
        etl.query "DELETE FROM etl_source WHERE amount < 0"
      end

      etl.etl do |etl|
        etl.query %[
          REPLACE INTO etl_destination
          SELECT name, SUM(amount) FROM etl_source
          GROUP BY name]
      end

      etl.after_etl do |etl|
        etl.query %[
          UPDATE etl_destination
          SET name = CONCAT("SUPER ", name)
          WHERE total_amount > 115]
      end

      etl.run

      connection
        .query("SELECT * FROM etl_destination ORDER BY total_amount DESC")
        .to_a
        .should == [
          {'name' => 'SUPER Jack', 'total_amount' => 120},
          {'name' => 'Jeff',       'total_amount' => 110},
          {'name' => 'Nick',       'total_amount' => 90},
          {'name' => 'Ryan',       'total_amount' => 50}]
    end
  end

  describe '#run with operations specified for exclusion' do
    let(:connection) { stub }
    let(:etl)        { described_class.new connection: connection, logger: logger }

    it "does not call the specified method" do
      etl.ensure_destination {}
      etl.should_not_receive(:ensure_destination)
      etl.run except: :ensure_destination
    end
  end

  context "with iteration" do
    describe '#run over full table' do
      let(:connection) { test_connection }
      let(:etl)        { described_class.new connection: connection, logger: logger }

      before { reset_test_env connection }
      after  { connection.close }

      it "executes the specified sql in the appropriate order and ETLs properly" do
        etl.ensure_destination do |etl|
          etl.query %[
            CREATE TABLE etl_destination (
                id INT NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0
              , PRIMARY KEY (id))]
        end

        etl.before_etl do |etl|
          etl.query "DELETE FROM etl_source WHERE amount < 0"
        end

        etl.start do |etl|
          etl.query(
            "SELECT COALESCE(MAX(id), 0) AS the_start FROM etl_destination"
          ).to_a.first['the_start']
        end

        etl.step do
          1
        end

        etl.stop do |etl|
          etl.query(
            "SELECT MAX(id) AS the_stop FROM etl_source"
          ).to_a.first['the_stop']
        end

        etl.etl do |etl, lbound, ubound|
          etl.query %[
            REPLACE INTO etl_destination
            SELECT id, name, amount FROM etl_source s
            WHERE s.id >= #{lbound}
              AND s.id <  #{ubound}]
        end

        etl.after_etl do |etl|
          etl.query %[
            UPDATE etl_destination
            SET name = CONCAT("SUPER ", name)
            WHERE id <= 1]
        end

        etl.run

        connection
          .query("SELECT * FROM etl_destination ORDER BY id ASC")
          .to_a
          .should == [
            {'id' => 1, 'name' => 'SUPER Jeff', 'amount' => 100},
            {'id' => 2, 'name' => 'Ryan',       'amount' => 50},
            {'id' => 3, 'name' => 'Jack',       'amount' => 75},
            {'id' => 4, 'name' => 'Jeff',       'amount' => 10},
            {'id' => 5, 'name' => 'Jack',       'amount' => 45},
            {'id' => 7, 'name' => 'Nick',       'amount' => 90}]
      end
    end

    describe '#run over part of table' do
      let(:connection) { test_connection }
      let(:etl)        { described_class.new connection: connection, logger: logger }

      before { reset_test_env connection }
      after  { connection.close }

      it "executes the specified sql in the appropriate order and ETLs properly" do
        etl.ensure_destination do |etl|
          etl.query %[
            CREATE TABLE etl_destination (
                id INT NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0
              , PRIMARY KEY (id))]
        end

        etl.before_etl do |etl|
          etl.query "DELETE FROM etl_source WHERE amount < 0"
        end

        etl.start do
          4
        end

        etl.step do
          1
        end

        etl.stop do |etl|
          etl.query(
            "SELECT MAX(id) AS the_stop FROM etl_source"
          ).to_a.first['the_stop']
        end

        etl.etl do |etl, lbound, ubound|
          etl.query %[
            REPLACE INTO etl_destination
            SELECT id, name, amount FROM etl_source s
            WHERE s.id >= #{lbound}
              AND s.id <  #{ubound}]
        end

        etl.run

        connection
          .query("SELECT * FROM etl_destination ORDER BY id ASC")
          .to_a.should == [
            {'id' => 4, 'name' => 'Jeff', 'amount' => 10},
            {'id' => 5, 'name' => 'Jack', 'amount' => 45},
            {'id' => 7, 'name' => 'Nick', 'amount' => 90}]
      end
    end

    describe "#run over gappy data" do
      let(:connection) { test_connection }
      let(:etl)        { described_class.new connection: connection, logger: logger }

      before do
        reset_test_env(connection) do |connection|
          connection.query %[
            CREATE TABLE etl_source (
                id INT NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0
              , PRIMARY KEY (id))]

          connection.query %[
            INSERT INTO etl_source (id, name, amount)
            VALUES
              (1,  'Jeff',  100),
              (2,  'Ryan',  50),
              (13, 'Jack',  75),
              (14, 'Jeff',  10),
              (15, 'Jack',  45),
              (16, 'Nick', -90),
              (17, 'Nick',  90)]
        end
      end

      after { connection.close }

      it "executes the specified sql in the appropriate order without getting stuck" do
        etl.ensure_destination do |etl|
          etl.query %[
            CREATE TABLE etl_destination (
                id INT NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0
              , PRIMARY KEY (id))]
        end

        etl.before_etl do |etl|
          etl.query "DELETE FROM etl_source WHERE amount < 0"
        end

        etl.start do |etl|
          1
        end

        etl.step do
          1
        end

        etl.stop do |etl|
          etl.query(
            "SELECT MAX(id) AS the_stop FROM etl_source"
          ).to_a.first['the_stop']
        end

        etl.etl do |etl, lbound, ubound|
          etl.query %[
            REPLACE INTO etl_destination
            SELECT
                id
              , name
              , amount
            FROM etl_source s
            WHERE s.id >= #{lbound}
              AND s.id <  #{ubound}]
        end

        etl.run

        connection
          .query("SELECT * FROM etl_destination ORDER BY id ASC")
          .to_a
          .should == [
            {'id' => 1,  'name' => 'Jeff', 'amount' => 100},
            {'id' => 2,  'name' => 'Ryan', 'amount' => 50},
            {'id' => 13, 'name' => 'Jack', 'amount' => 75},
            {'id' => 14, 'name' => 'Jeff', 'amount' => 10},
            {'id' => 15, 'name' => 'Jack', 'amount' => 45},
            {'id' => 17, 'name' => 'Nick', 'amount' => 90}]
      end
    end

    describe "#run over date data" do
      let(:connection) { test_connection }
      let(:etl)        { described_class.new connection: connection, logger: logger }

      before do
        reset_test_env(connection) do |connection|
          connection.query %[
            CREATE TABLE etl_source (
                the_date DATE NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0)]

          connection.query %[
            INSERT INTO etl_source (the_date, name, amount)
            VALUES
              ('2012-01-01', 'Jeff', 100),
              ('2012-01-01', 'Ryan', 50),
              ('2012-01-01', 'Jack', 75),
              ('2012-01-01', 'Jeff', 10),
              ('2012-01-02', 'Jack', 45),
              ('2012-01-02', 'Nick', -90),
              ('2012-01-02', 'Nick', 90)]
        end
      end

      after { connection.close }

      it "executes the specified sql in the appropriate order and ETLs properly" do
        etl.ensure_destination do |etl|
          etl.query %[
            CREATE TABLE etl_destination (
                the_date DATE NOT NULL
              , name VARCHAR(10)
              , total_amount INT(11) DEFAULT 0
              , PRIMARY KEY (the_date, name))]
        end

        etl.before_etl do |etl|
          etl.query "DELETE FROM etl_source WHERE amount < 0"
        end

        etl.start do |etl|
          etl.query(%[
            SELECT COALESCE(MAX(the_date), DATE('2012-01-01')) AS the_start
            FROM etl_destination
          ]).to_a.first['the_start']
        end

        etl.step do
          1.day
        end

        etl.stop do |etl|
          etl.query(
            "SELECT MAX(the_date) AS the_stop FROM etl_source"
          ).to_a.first['the_stop']
        end

        etl.etl do |etl, lbound, ubound|
          etl.query %[
            REPLACE INTO etl_destination
            SELECT
                the_date
              , name
              , SUM(amount) AS total_amount
            FROM etl_source s
            WHERE s.the_date >= '#{lbound}'
              AND s.the_date <  '#{ubound}'
            GROUP BY
                the_date
              , name]
        end

        etl.run

        connection
          .query(%[
            SELECT
                the_date
              , name
              , total_amount
            FROM
              etl_destination
            ORDER BY
                the_date ASC
              , name ASC
          ]).to_a
            .should == [
              {'the_date' => Date.parse('2012-01-01'), 'name' => 'Jack', 'total_amount' => 75},
              {'the_date' => Date.parse('2012-01-01'), 'name' => 'Jeff', 'total_amount' => 110},
              {'the_date' => Date.parse('2012-01-01'), 'name' => 'Ryan', 'total_amount' => 50},
              {'the_date' => Date.parse('2012-01-02'), 'name' => 'Jack', 'total_amount' => 45},
              {'the_date' => Date.parse('2012-01-02'), 'name' => 'Nick', 'total_amount' => 90}]
      end
    end

    describe "#run over datetime data" do
      let(:connection) { test_connection }
      let(:etl)        { described_class.new connection: connection, logger: logger }

      before do
        reset_test_env(connection) do |connection|
          connection.query %[
            CREATE TABLE etl_source (
                the_datetime DATETIME NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0)]

          connection.query %[
            INSERT INTO etl_source (the_datetime, name, amount)
            VALUES
              ('2011-12-31 23:59:59', 'Jeff', 100),
              ('2012-01-01 00:01:00', 'Ryan', 50),
              ('2012-01-01 00:01:01', 'Jack', 75),
              ('2012-01-01 00:01:02', 'Jeff', 10),
              ('2012-01-02 00:02:00', 'Jack', 45),
              ('2012-01-02 00:02:01', 'Nick', -90),
              ('2012-01-02 00:02:02', 'Nick', 90)]
        end
      end

      after { connection.close }

      it "executes the specified sql in the appropriate order and ETLs properly" do
        etl.ensure_destination do |etl|
          etl.query %[
            CREATE TABLE etl_destination (
                the_datetime DATETIME NOT NULL
              , name VARCHAR(10)
              , amount INT(11) DEFAULT 0
              , PRIMARY KEY (the_datetime, name))]
        end

        etl.before_etl do |etl|
          etl.query "DELETE FROM etl_source WHERE amount < 0"
        end

        etl.start do |etl|
          etl.query(%[
            SELECT CAST(COALESCE(MAX(the_datetime), '2012-01-01 00:00:00') AS DATETIME) AS the_start
            FROM etl_destination
          ]).to_a.first['the_start']
        end

        etl.step do
          1.minute
        end

        etl.stop do |etl|
          etl.query(
            "SELECT MAX(the_datetime) AS the_stop FROM etl_source"
          ).to_a.first['the_stop']
        end

        etl.etl do |etl, lbound, ubound|
          etl.query %[
            REPLACE INTO etl_destination
            SELECT
                the_datetime
              , name
              , amount
            FROM etl_source s
            WHERE s.the_datetime >= '#{lbound}'
              AND s.the_datetime <  '#{ubound}']
        end

        etl.run

        connection
          .query(%[
            SELECT
                the_datetime
              , name
              , amount
            FROM
              etl_destination
            ORDER BY
                the_datetime ASC
              , name ASC
          ]).to_a
            .should == [
              {'the_datetime' => Time.parse('2012-01-01 00:01:00'), 'name' => 'Ryan', 'amount' => 50},
              {'the_datetime' => Time.parse('2012-01-01 00:01:01'), 'name' => 'Jack', 'amount' => 75},
              {'the_datetime' => Time.parse('2012-01-01 00:01:02'), 'name' => 'Jeff', 'amount' => 10},
              {'the_datetime' => Time.parse('2012-01-02 00:02:00'), 'name' => 'Jack', 'amount' => 45},
              {'the_datetime' => Time.parse('2012-01-02 00:02:02'), 'name' => 'Nick', 'amount' => 90}]
      end
    end
  end
end
