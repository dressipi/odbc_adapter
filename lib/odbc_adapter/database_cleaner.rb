class ODBCAdapter::Adapters::RedshiftODBCAdapter
  def truncate_table(table_name)
    execute("TRUNCATE TABLE #{quote_table_name(table_name)};")
  end

  # this is a bit of a misnomer - we don't cache
  # We query svv_table_info (which requires extra privileges) becauser
  # it only lists non empty tables and is thus much faster
  #
  def database_cleaner_table_cache
    begin
      select_rows( 'select schema, "table" from svv_table_info').map do |row|
        "#{quote_table_name(row[0])}.#{quote_table_name(row[1])}"
      end
    rescue ActiveRecord::StatementInvalid => e
      ActiveRecord::Base.logger&.info("falling back to information_schema.tables which is slower than svv_table_info: #{e.message}")
      
      query = <<~SQL
      SELECT table_schema, table_name
      FROM information_schema.tables 
      WHERE table_schema != 'information_schema' AND table_schema not like ('pg_%')
        and table_type = 'BASE TABLE'
      SQL
      select_rows( query ).map do |row|
        next if row[0] == 'information_schema' || row[0].start_with?('pg_')
        "#{quote_table_name(row[0])}.#{quote_table_name(row[1])}"
      end.compact
    end
  end      
end

