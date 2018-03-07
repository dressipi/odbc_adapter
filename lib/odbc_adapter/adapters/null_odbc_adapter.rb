module ODBCAdapter
  module Adapters
    # A default adapter used for databases that are no explicitly listed in the
    # registry. This allows for minimal support for DBMSs for which we don't
    # have an explicit adapter.
    class NullODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter
      BOOLEAN_TYPE = 'bool'.freeze
      PRIMARY_KEY  = 'SERIAL PRIMARY KEY'.freeze
      class BindSubstitution < Arel::Visitors::ToSql
        include Arel::Visitors::BindVisitor
      end

      # Override to handle booleans appropriately
      def native_database_types
        @native_database_types ||= super.merge(boolean: { name: 'bool' })
      end

      # A custom hook to allow end users to overwrite the type casting before it
      # is returned to ActiveRecord. Useful before a full adapter has made its way
      # back into this repository.
      # The internal type conversion mapping can be found here
      # https://www.easysoft.com/developer/languages/c/examples/ListDataTypes.html
      # define SQL_UNKNOWN_TYPE        0
      # define SQL_CHAR                1
      # define SQL_NUMERIC             2
      # define SQL_DECIMAL             3
      # define SQL_INTEGER             4
      # define SQL_SMALLINT            5
      # define SQL_FLOAT               6
      # define SQL_REAL                7
      # define SQL_DOUBLE              8
      # define SQL_DATETIME            9      ODBCVER >= 0x0300
      # define SQL_DATE                9
      # define SQL_INTERVAL            10     ODBCVER >= 0x0300
      # define SQL_TIME                10
      # define SQL_TIMESTAMP           11
      # define SQL_VARCHAR             12

      # define SQL_TYPE_DATE           91     ODBCVER >= 0x0300
      # define SQL_TYPE_TIME           92     ODBCVER >= 0x0300
      # define SQL_TYPE_TIMESTAMP      93     ODBCVER >= 0x0300

      # define SQL_LONGVARCHAR         (-1)
      # define SQL_BINARY              (-2)
      # define SQL_VARBINARY           (-3)
      # define SQL_LONGVARBINARY       (-4)
      # define SQL_BIGINT              (-5)
      # define SQL_TINYINT             (-6)
      # define SQL_BIT                 (-7)
      # define SQL_WCHAR               (-8)
      # define SQL_WVARCHAR            (-9)
      # define SQL_WLONGVARCHAR        (-10)
      # define SQL_GUID      (-11)  ODBCVER >= 0x0350
      def dbms_type_cast(_columns, values)
        values.map do |value| 
          _columns.zip(value).map do |c, v|
            # Custom formatting for time object
            # the default will be with timezone and without subsec
            if v.is_a? Time
              result = v.to_formatted_s(:db)
              if v.subsec > 0
                result + v.strftime(".%3N")
              else
                result
              end
            # the internal type for ODBC::SQL_FLOAT is 6
            # and we need to do trimming here
            # (i.e. removing trailing zeroes)
            elsif type_is_?(c, 6)
              trim(v)
            # the internal type for ODBC::SQL_REAL is 7
            # the value returned with precision 9
            # and the rest will be floating discrepancy
            # however REAL has a scale up to 6
            # this is a workaround
            elsif type_is_?(c, 7)
              BigDecimal.new(v.to_s).add(0, 6).to_f
            # We have boolean type cast to smallint (SQL_SMALLINT as type 5)
            # for the comfort of Amazon DMS
            # now we need to convert it back
            # But we also have boolean type which is 
            elsif (['1', '0'].include?(v) && is_bool_candidate(c[1])) || type_is_?(c, 5)
              v.to_i == 1 ? true : false
            else
              v
            end
          end
        end
      end

      def trim(num)
        i, f = num.to_i, num.to_f
        i == f ? i : f
      end

      # returns true if it's Boolean 
      # type is 12 (SQL_VARCHAR) and have certain rule on Redshift
      def is_bool_candidate(column)
        column.type == 12 && column.length == 5 && column.precision == 5 && column.scale == 0
      end

      def type_is_?(column, type_num)
        column[1].type == type_num
      end

      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        BindSubstitution.new(self)
      end

      def select_rows(sql, cast_values: false)
        cast_values ? select_all(sql).rows : type_uncast(select_all(sql).rows)
      end

      def select_values(sql)
        select_rows(sql).map(&:first)
      end

      def select_value(sql)
        select_values(sql).first
      end

      def truncate_table(table_name)
        execute("TRUNCATE TABLE #{table_name}")
      end

      # We cannot replace tables_to_truncate method in 
      # DatabaseCleaner::ActiveRecord::Truncation
      # so here is a little hack to retrieve the needed
      # tables for truncation
      def truncate_tables(table_names)
        table_select_sql = <<-SQL
          SELECT table_name 
          FROM information_schema.tables 
          WHERE table_schema = (SELECT current_schema()) 
            and table_type = 'BASE TABLE'
        SQL
        # Overwrite the original table_names variable
        # because that entails all the tables/views
        # in the entire database
        table_names = exec_query(table_select_sql).rows

        return if table_names.nil? || table_names.empty?
        table_names.each do |table_name|
          truncate_table(table_name)
        end
      end

      def type_uncast(values)
        values.map do |value| 
          value.map do |v| 
            if v.nil? 
              nil
            elsif v.is_a? DateTime 
              v.to_formatted_s(:db) 
            elsif !!v == v
              v ? 't' : 'f'
            else 
              v.to_s
            end
          end
        end
      end

      # Explicitly turning off prepared_statements in the null adapter because
      # there isn't really a standard on which substitution character to use.
      def prepared_statements
        false
      end

      # Turning off support for migrations because there is no information to
      # go off of for what syntax the DBMS will expect.
      def supports_migrations?
        false
      end
    end
  end
end
