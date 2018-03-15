module ODBCAdapter
  if ActiveRecord::VERSION::MAJOR >= 5
    class Column < ActiveRecord::ConnectionAdapters::Column
      attr_reader :native_type

      # Add the native_type accessor to allow the native DBMS to report back what
      # it uses to represent the column internally.
      # rubocop:disable Metrics/ParameterLists
      def initialize(name, default, sql_type_metadata = nil, null = true, table_name = nil, native_type = nil, default_function = nil, collation = nil)
        super(name, default, sql_type_metadata, null, table_name, default_function, collation)
        @native_type = native_type
      end
    end

  else
    class Column < ActiveRecord::ConnectionAdapters::Column
      def initialize(name, default, cast_type, sql_type, null, native_type, scale, limit)
        @name        = name
        @default     = default
        @cast_type   = cast_type
        @sql_type    = sql_type
        @null        = null
        @native_type = native_type

        if [ODBC::SQL_DECIMAL, ODBC::SQL_NUMERIC].include?(sql_type)
          set_numeric_params(scale, limit)
        end
      end

      private

      def set_numeric_params(scale, limit)
        @cast_type.instance_variable_set(:@scale, scale || 0)
        @cast_type.instance_variable_set(:@precision, limit)
      end
    end
  end
end
