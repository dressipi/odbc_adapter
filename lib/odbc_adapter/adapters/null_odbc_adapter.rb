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
      def dbms_type_cast(_columns, values)
        # values.map{ |value| value.map{ |v| (v.is_a? Time) ? DateTime.parse(v.to_formatted_s(:db)) : v } }
        values.map do |value| 
          _columns.zip(value).map do |c, v|
            if v.is_a? Time
              DateTime.parse(v.to_formatted_s(:db))
            # Convert '1' and '0' to 't' and 'f'
            # this is done if the target 
            elsif ['1', '0'].include?(v) && is_bool_candidate(c[1]) 
              v == '1' ? 't' : 'f'
            else
              v
            end
          end
        end
      end

      # Boolean returns 
      def is_bool_candidate(column)
        column.length == 5 && column.precision == 5 && column.scale == 0
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

      def truncate_table(table_name)
        require 'pry'
        pry
        execute("TRUNCATE TABLE #{quote_table_name(table_name)}")
      end

      def type_uncast(values)
        values.map do |value| 
          value.map do |v| 
            if v.nil? 
              nil
            elsif v.is_a? DateTime 
              v.to_formatted_s(:db) 
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
