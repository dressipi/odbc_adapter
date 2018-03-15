require 'test_helper'

class MetadataTest < Minitest::Test
  
  def test_string_value_encoding
    metadata = ::ODBCAdapter::DatabaseMetadata.new(User.connection.raw_connection)
    if ODBC::UTF8
      assert_equal(Encoding::UTF_8, metadata.dbms_name.encoding)
      assert(metadata.dbms_name.valid_encoding?)
    end
  end
end
