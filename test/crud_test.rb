require 'test_helper'

class CRUDTest < Minitest::Test
  def test_creation
    with_transaction do
      User.create(first_name: 'foo', last_name: 'bar')
      assert_equal 7, User.count
    end
  end

  def test_update
    with_transaction do
      user = User.first
      user.letters = 47
      user.save!

      assert_equal 47, user.reload.letters
    end
  end

  def test_destroy
    with_transaction do
      User.last.destroy
      assert_equal 5, User.count
    end
  end

  def test_select_all
    expected = [{ "first_name" => 'Ash', "last_name" => 'Hepburn', "letters" => 10  },
                { "first_name" => 'Kevin', "last_name" => 'Deisz', "letters" => 10 },
                { "first_name" => 'Michal', "last_name" => 'Klos', "letters" => 10 },
                ]

    assert_equal expected,
                User.connection.select_all("SELECT first_name, last_name, letters from users where letters = 10 ORDER by first_name ASC").to_hash
  end

  def test_select_rows
    expected = [[ 'Ash',  'Hepburn',  10 ],
                [ 'Kevin',  'Deisz',  10 ],
                [ 'Michal',  'Klos',  10 ],
                ]

    assert_equal expected,
                User.connection.select_rows("SELECT first_name, last_name, letters from users where letters = 10 ORDER by first_name ASC")
  end

  private

  def with_transaction(&_block)
    User.transaction do
      yield
      raise ActiveRecord::Rollback
    end
  end
end
