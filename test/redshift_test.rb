require 'test_helper'
require 'odbc_adapter/database_cleaner'
class RedshiftTest < Minitest::Test

  if ActiveRecord::Base.connection.is_a?(ODBCAdapter::Adapters::RedshiftODBCAdapter)

    class BooleanFoo < ActiveRecord::Base
    end

    def test_actual_booleans
      raw = ActiveRecord::Base.connection.select_rows("select id, published from todos order by id asc limit 1")
      assert_equal([[1, true]], raw)
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
        BooleanFoo.reset_column_information
        BooleanFoo.create(flag: true, name: "is_true")
        BooleanFoo.create(flag: false, name: "is_false")

        assert_equal(true, BooleanFoo.find_by_name("is_true").flag)
        assert_equal(false,BooleanFoo.find_by_name("is_false").flag)

        raw = BooleanFoo.connection.select_rows("select name,flag from boolean_foos order by id")
        assert_equal([["is_true", true], ["is_false", false]], raw)

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

    describe 'database cleaner_additions' do
      def setup
        @connection = User.connection

        @connection.create_table(:boolean_foos, force: true) do |t|
          t.boolean :flag
          t.string :name
        end
        BooleanFoo.reset_column_information
      end

      def teardown
        @connection.drop_table("boolean_foos")
      end

      def test_truncate
        BooleanFoo.create(flag: true, name: "is_true")
        @connection.truncate_table("boolean_foos")
        assert_equal(BooleanFoo.count, 0)
      end

      #requires that the user have 
      #select permission on svv_table_info
      #
      def test_optimized_database_cleaner_table_cache
        assert_equal(['"public"."ar_internal_metadata"', '"public"."todos"', '"public"."users"'], @connection.database_cleaner_table_cache.sort)
        BooleanFoo.create(flag: true, name: "is_true")
        assert_equal(['"public"."ar_internal_metadata"', '"public"."boolean_foos"', '"public"."todos"', '"public"."users"'], @connection.database_cleaner_table_cache.sort)
        @connection.truncate_table("boolean_foos")
        assert_equal(['"public"."ar_internal_metadata"', '"public"."todos"', '"public"."users"'], @connection.database_cleaner_table_cache.sort)
      end

    end


    describe 'set search path' do
      class ConnectionWithOption < ActiveRecord::Base
        self.abstract_class = true
      end

      def test_can_be_set_from_configuration
        ConnectionWithOption.establish_connection(
          ActiveRecord::Base.connection_config.merge(:schema_search_path => "information_schema, public")
        )
        assert_equal({"search_path" => "information_schema, public"},
                       ConnectionWithOption.connection.select_one("show search_path"))
      end

      def test_can_be_set_with_accessor
        ConnectionWithOption.establish_connection(ActiveRecord::Base.connection_config)
        ConnectionWithOption.connection.schema_search_path = "information_schema, public"
        assert_equal({"search_path" => "information_schema, public"},
                       ConnectionWithOption.connection.select_one("show search_path"))

      end
    end

    describe 'quote_table_name' do      
      def setup
        @connection = User.connection
      end

      def test_already_quoted_names_are_preserved
        assert_equal '"table.name"', @connection.quote_table_name('"table.name"')
        assert_equal '"schema.name"."table.name"', @connection.quote_table_name('"schema.name"."table.name"')
      end

      def test_quotes_bare_table_name
        assert_equal '"table_name"', @connection.quote_table_name("table_name")
      end

      def test_quotes_qualified_table_name
        assert_equal '"schema_name"."table_name"', @connection.quote_table_name("schema_name.table_name")
      end

      def test_quotes_qualified_name_where_one_portion_is_quoted
        assert_equal '"schema_name"."table.name"', @connection.quote_table_name('schema_name."table.name"')
        assert_equal '"schema.name"."table_name"', @connection.quote_table_name('"schema.name".table_name')
      end
    end
  end
end