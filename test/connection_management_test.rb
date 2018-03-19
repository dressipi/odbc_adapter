require 'test_helper'

class ConnectionManagementTest < Minitest::Test
  def test_connection_management
    assert conn.active?

    conn.disconnect!
    refute conn.active?

    conn.disconnect!
    refute conn.active?

    conn.reconnect!
    assert conn.active?
  ensure
    conn.reconnect!
  end


  def test_reconnect_within_a_transaction
    assert conn.active?
    conn.transaction do
      conn.execute("select 1")
      conn.reconnect!
    end
    assert conn.active?
  end

  private

  def conn
    ActiveRecord::Base.connection
  end
end
