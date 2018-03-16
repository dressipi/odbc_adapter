require 'test_helper'

class ConnectionStringTest < Minitest::Test

  def test_simple_case
    assert_equal(
      {'Foo' => 'Bar', 'Baz' => 'Barf'},
       ActiveRecord::ConnectionAdapters::ODBCAdapter.odbc_parse_connection_string("Foo=Bar;Baz=Barf")
    )
  end

  def test_single_quoted
    assert_equal(
      {'Foo' => 'Bar', 'Baz' => 'Barf;Baz'},
       ActiveRecord::ConnectionAdapters::ODBCAdapter.odbc_parse_connection_string("Foo=Bar;Baz='Barf;Baz'")
    )
  end

  def test_double_quoted
    assert_equal(
      {'Foo' => 'Bar', 'Baz' => "Barf ' Baz"},
       ActiveRecord::ConnectionAdapters::ODBCAdapter.odbc_parse_connection_string(%Q[Foo=Bar;Baz="Barf ' Baz"])
    )
  end

end