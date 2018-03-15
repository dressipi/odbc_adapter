require 'test_helper'

class CalculationsTest < Minitest::Test
  def test_count
    assert_equal 6, User.count
    assert_equal 10, Todo.count
    assert_equal 3, User.find(1).todos.count
  end

  if User.connection.is_a?(ODBCAdapter::Adapters::RedshiftODBCAdapter)
    # On redshift, averaging an integer column returns an integer
    # https://docs.aws.amazon.com/redshift/latest/dg/r_AVG.html
    def test_average
      assert_equal 10, User.average(:letters)
      assert_equal 10.33, User.average("cast(letters as float)").round(2)
    end
  else
    def test_average
      assert_equal 10.33, User.average(:letters).round(2)
    end
  end
end
