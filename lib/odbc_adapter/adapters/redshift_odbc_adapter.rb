module ODBCAdapter
  module Adapters
    class RedshiftODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter

      BOOLEAN_TYPE = 'bool'

      class_attribute :emulate_booleans

      self.emulate_booleans = false

      def initialize(connection, logger, config, database_metadata)
        super
        @emulate_booleans = !! config[:emulate_booleans]
      end

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

      if ActiveRecord::VERSION::MAJOR < 5
        alias_method :visitor, :arel_visitor
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




      # Maps logical Rails types to redshift-specific data types.
      if ActiveRecord::VERSION::MAJOR >= 5
        def type_to_sql(type, limit: nil, precision: nil, scale: nil, array: nil, **)
          sql = \
            case type.to_s
            when "integer"
              integer_type_to_sql(limit)
            else
              super
            end

          sql
        end
      else
        def type_to_sql(type, limit = nil, precision = nil, scale = nil)
          sql = \
            case type.to_s
            when "integer"
              integer_type_to_sql(limit)
            else
              super
            end
          sql
        end
      end


      def quoted_true
        if emulate_booleans
          "1".freeze
        else
          super
        end
      end


      def quoted_false
        if emulate_booleans
          "0".freeze
        else
          super
        end
      end

      def unquoted_true
        if emulate_booleans
          1
        else
          super
        end
      end


      def unquoted_false
        if emulate_booleans
          0
        else
          super
        end
      end

      protected

      def integer_type_to_sql(limit)
        case limit
        when 1, 2; "smallint"
        when nil, 3, 4; "integer"
        when 5..8; "bigint"
        else raise(ActiveRecordError, "No integer type has byte size #{limit}. Use a numeric with scale 0 instead.")
        end
      end

      def initialize_type_map(map)
        super
        map.register_type ODBC::SQL_SMALLINT, ActiveRecord::Type::Boolean.new if emulate_booleans
      end

      def dbms_type_cast(columns, values)
        boolean_indices = boolean_column_indices(columns)
        if boolean_indices.any?
          values.each do |row|
            boolean_indices.each do |index|
              row[index] = case row[index]
                when 1,'1' then true
                when 0,'0' then false
                when nil then nil
                else
                  raise "Unexpected boolean value #{row[index].inspect}"
                end
            end
          end
        end
        values
      end

      def boolean_column_indices(columns)
        columns.each.
          with_index.
          select do |col, _index| 
            (emulate_booleans && col.type == ODBC::SQL_SMALLINT) ||
            col.type == ODBC::SQL_BIT ||
            string_masquerading_as_boolean?(col)
          end.
          map {|_col, index| index}
      end

      # although redshift has a boolean type, driver returns booleans as strings
      #
      def string_masquerading_as_boolean?(column)
        column.type == ODBC::SQL_VARCHAR &&
        column.length == 5
      end
    end
  end
end
