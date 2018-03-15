module ODBCAdapter
  # Caches SQLGetInfo output
  class DatabaseMetadata
    FIELDS = %i[
      SQL_DBMS_NAME
      SQL_DBMS_VER
      SQL_IDENTIFIER_CASE
      SQL_QUOTED_IDENTIFIER_CASE
      SQL_IDENTIFIER_QUOTE_CHAR
      SQL_MAX_IDENTIFIER_LEN
      SQL_MAX_TABLE_NAME_LEN
      SQL_USER_NAME
      SQL_DATABASE_NAME
    ].freeze

    attr_reader :values

    def initialize(connection)
      @values = Hash[FIELDS.map { |field| [field, fix_encoding(connection.get_info(ODBC.const_get(field)))] }]
    end

    def adapter_class
      ODBCAdapter.adapter_for(dbms_name)
    end

    def upcase_identifiers?
      @upcase_identifiers ||= (identifier_case == ODBC::SQL_IC_UPPER)
    end

    # A little bit of metaprogramming magic here to create accessors for each of
    # the fields reported on by the DBMS.
    FIELDS.each do |field|
      define_method(field.to_s.downcase.gsub('sql_', '')) do
        value_for(field)
      end
    end

    private

    def fix_encoding(value)
      # when odbc is in unicode mode, it doesn't encode the results of get_info properly - the 
      # underlying utf-16 encoding leaks through as ascii-8but
      # This is true as of 0.99999
      #
      if ODBC::UTF8 && value.is_a?(String) && value.encoding == Encoding::BINARY
        value.force_encoding('UTF-16LE').encode('UTF-8')
      else
        value
      end
    end

    def value_for(field)
      values[field]
    end
  end
end
