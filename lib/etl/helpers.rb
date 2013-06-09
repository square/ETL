class ETL
  module Helpers
    # max_for returns the max value for the passed in column as found in the
    # specified database.table. If there is not currently a max, we use COALESCE
    # and a default value. You can specify a :default_floor value or the method
    # will try to derive it for you.
    #
    # Note: we try to detect if we want a date return type via the #datetype?
    # check.
    #
    # If this is found we wrap the whole SELECT clause in a DATE so it is cast
    # accordingly.
    def max_for options = {}
      database = options[:database]
      table    = options[:table]
      column   = options[:column]

      default_value = options[:default_floor] ||
                        default_floor_for(column)

      if date? default_value
        default_value = "DATE('#{default_value}')"
        caster = ->(str) { "DATE(#{str})" }
      end

      max_sql_clause = "IFNULL(MAX(#{table}.#{column}), #{default_value})"
      max_sql_clause = caster.(max_sql_clause) if caster

      sql = <<-EOS
        SELECT #{max_sql_clause} AS the_max
        FROM #{database}.#{table}
      EOS
      sql += " WHERE #{options[:conditions]}" if options[:conditions]

      query(sql).to_a.first['the_max']
    end

  private

    def date? val
      val =~ /^\d{4}-\d{1,2}-\d{1,2}( \d{2}:\d{2}:\d{2}( ((-|\+)\d+)| UTC)?)?$/
    end

    def default_floor_for column
      case column
      when /_at$/
        return '1970-01-01'
      when /_date$/
        return '1970-01-01'
      when /(^id$|_id$)/
        return 0
      else
        raise ArgumentError, "could not determine a default for #{column}"
      end
    end
  end
end
