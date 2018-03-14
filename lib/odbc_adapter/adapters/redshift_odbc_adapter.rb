module ODBCAdapter
  module Adapters
    class RedshiftODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter

      BOOLEAN_TYPE = 'bool'

      def native_database_types
        {
          :primary_key=> "BIGINT IDENTITY(1,1) PRIMARY KEY",
          :string => { :name => "VARCHAR", :limit=>255},
          :text => { :name => "text"},
          :integer => { :name => "int4"},
          :decimal => { :name => "numeric"},
          :float => { :name => "float8"},
          :datetime => { :name => "timestamp"},
          :timestamp => { :name => "timestamp"},
          :time => { :name => "timestamp"},
          :date => { :name => "date"},
          :binary => { :name => "bytea"},
          :boolean => { :name => "boolean"}
        }
      end

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
        true
      end

      # Create a new redshift database. Options include <tt>:owner</tt>,
      # <tt>:connection_limit</tt>
      #
      def create_database(name, options = {})
       
        option_string = options.symbolize_keys.sum do |key, value|
          case key
          when :owner
            " OWNER = \"#{value}\""
          when :connection_limit
            " CONNECTION LIMIT = #{value}"
          else
            ''
          end
        end

        execute("CREATE DATABASE #{quote_table_name(name)}#{option_string}")
      end

      # Drops a redshift database.
      #
      # Example:
      #   drop_database 'rails_development'
      def drop_database(name)
        execute "DROP DATABASE #{quote_table_name(name)}"
      end

      # Renames a table.
      def rename_table(name, new_name)
        execute("ALTER TABLE #{quote_table_name(name)} RENAME TO #{quote_table_name(new_name)}")
      end

      def change_column(table_name, column_name, type, options = {})
        raise NotImplementedError, "Redshift does not allow modifying columns"
      end

      def change_column_default(table_name, column_name, default)
        raise NotImplementedError, "Redshift does not allow modifying columns"
      end

      def rename_column(table_name, column_name, new_column_name)
        execute("ALTER TABLE #{table_name} RENAME #{column_name} TO #{new_column_name}")
      end

      def remove_index!(_table_name, index_name)
        execute("DROP INDEX #{quote_table_name(index_name)}")
      end

      def rename_index(_table_name, old_name, new_name)
        execute("ALTER INDEX #{quote_column_name(old_name)} RENAME TO #{quote_table_name(new_name)}")
      end

    end
  end
end
