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

      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        BindSubstitution.new(self)
      end

      def select_rows(sql, cast_values: false)
        cast_values ? select_all(sql).rows : type_uncast(select_all(sql).rows)
      end

      def type_uncast(values)
        values.map{|value| value.map do |v| 
          if v.nil? 
            nil
          elsif v.is_a? DateTime 
            v.to_formatted_s(:db) 
          else 
            v.to_s
          end
        end
        }
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
