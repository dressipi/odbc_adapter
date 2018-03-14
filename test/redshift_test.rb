require 'test_helper'

class RedshiftTest < Minitest::Test

  if ActiveRecord::Base.connection.is_a?(ODBCAdapter::Adapters::RedshiftODBCAdapter)

    class BooleanFoo < ActiveRecord::Base
    end

    describe 'emulate_booleans = true' do

      def setup
        @connection = User.connection
        @connection.emulate_booleans = true
        @connection.send(:reload_type_map)
      end

      def test_maps_smallints_to_booleans
        @connection.create_table(:boolean_foos, force: true) do |t|
          t.integer :flag, limit: 2
          t.string :name
        end

        BooleanFoo.create(flag: true, name: "is_true")
        BooleanFoo.create(flag: false, name: "is_false")

        assert_equal(true, BooleanFoo.find_by_name("is_true").flag)
        assert_equal(false,BooleanFoo.find_by_name("is_false").flag)

        @connection.emulate_booleans = false
        @connection.send(:reload_type_map)
        BooleanFoo.reset_column_information
        assert_equal(1, BooleanFoo.find_by_name("is_true").flag)
        assert_equal(0, BooleanFoo.find_by_name("is_false").flag)

        assert_equal("is_true", BooleanFoo.find_by_flag(true).name)

      end

      def teardown
        @connection.drop_table("boolean_foos")
        @connection.emulate_booleans = true
        @connection.send(:reload_type_map)
      end
    end
  end
end