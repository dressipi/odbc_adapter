module ODBCAdapter
  module Adapters
    class RedshiftODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter
      BOOLEAN_TYPE = 'bool'.freeze
      PRIMARY_KEY  = 'BIGINT IDENTITY(0,1) PRIMARY KEY'.freeze

      class BindSubstitution < Arel::Visitors::ToSql
        include Arel::Visitors::BindVisitor
      end

      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        BindSubstitution.new(self)
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
