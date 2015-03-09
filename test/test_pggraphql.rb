require "test-unit"
require "pggraphql"

module PgGraphQl
  class PgGraphQlTest < ::Test::Unit::TestCase

    def token(sql)
      sql = sql.gsub(/\n/, " ").gsub("=", " = ").gsub(/,/, " , ").gsub("(", " ( ").gsub(")", " ) ").gsub(/[ ]+/, " ")
      sql.strip.split(" ").map{|e| e.strip}.reject{|e| e.empty?}
    end

    def to_sql(query, print_sql=nil, &block)
      s = Schema.new(&block)
      s.class.send(:define_method, :wrap_root) do |unwrapped_sql|
        unwrapped_sql
      end
      
      sql = s.to_sql(query)
      puts sql if print_sql
      sql
    end

    def test_simple
      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email
                from users
                where id = 1 limit 1) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_pk_array_empty
      res = to_sql({user: {id: [], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select id,
                  email
                from users) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_pk_array_one
      res = to_sql({user: {id: [1], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select id,
                  email
                from users where id in (1)) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_pk_array_multiple
      res = to_sql({user: {id: [1,2], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select id,
                  email
                from users where id in (1,2)) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_filter
      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email], filter: "id > 100"
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email
                from users
                where id = 1 and (id > 100) limit 1) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_multiple
      res = to_sql({user: {id: 1, email: "email"}, educator: {id: "id"}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email]
        s.type :educator
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email
                from users
                where id = 1 limit 1) x) as value
        union all
        select 'educator'::text as key,
          (select to_json(x.*)
            from (select id
                from educators limit 1) x) as value
      SQL
      ), token(res)
    end

    #####################
    # inherit
    #####################

    def test_inherit
      res = to_sql({
        products: {
          id: [], 
          type: nil,
          clickout__destination_url: nil,
          download__download_url: nil,
          download__users: {
            id: "id",
            orders: {
              id: "id"
            }
          }
        }
      }) do |s|
        s.root :product

        s.type :user, fields: [:id, :email] do |t|
          t.many :orders, fk: "user_id = users.id"
        end

        s.type :order

        s.type :product, fields: [:id, :type, :clickout__destination_url, :download__download_url] do |t|

          t.map :id, "products.id"

          t.subtype :download, table: :product_downloads, fk: "download.id = products.id and products.type = 'download'"
          t.subtype :clickout, table: :product_clickouts, fk: "clickout.id = products.id and products.type = 'clickout'"

          t.many :download__users, type: :user, fk: "id = download.id"
        end

      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select products.id as id,
                  type,
                  clickout.destination_url as clickout__destination_url,
                  download.download_url as download__download_url,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select id,
                          (select to_json(coalesce(json_agg(x.*), '[]'::json))
                            from (select id
                                from orders
                                where (user_id = users.id)) x) as orders
                        from users
                        where (id = download.id)) x) as download__users
                from products
                left join product_downloads as download on (download.id = products.id
                    and products.type = 'download')
                left join product_clickouts as clickout on (clickout.id = products.id
                    and products.type = 'clickout')) x) as value
      SQL
      ), token(res)
      
    end


    #####################
    # one
    #####################


    def test_link_one
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.one :address, fk: "id = users.address_id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(x.*)
                    from (select id
                        from addresses
                        where (id = users.address_id) limit 1) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_missing_fk
      assert_raise_message "missing :fk on link :address" do
        to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
          s.root :user
          s.type :user, fields: [:id, :email] do |t|
            t.one :address
          end
          s.type :address
        end
      end      
    end


    def test_link_one_fk_sql
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.one :address, fk: "id = (select 100)"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(x.*)
                    from (select id
                        from addresses
                        where (id = (select 100)) limit 1) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_filter
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.one :address, fk: "user_id = users.id", filter: "id > 100"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(x.*)
                    from (select id
                        from addresses
                        where (user_id = users.id) and (id > 100) limit 1) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_order_by
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.one :address, fk: "user_id = users.id", order_by: "id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(x.*)
                    from (select id
                        from addresses
                        where (user_id = users.id) order by id desc limit 1) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    # #####################
    # # one-in-one
    # #####################

    def test_link_one_in_one
      res = to_sql({user: {id: 1, email: "email", address: {id: "id", country: {id: "id"}}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.one :address, fk: "user_id = users.id"
        end
        s.type :address do |t|
          t.one :country, fk: "id = addresses.country_id"
        end
        s.type :country
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(x.*)
                    from (select id,
                          (select to_json(x.*)
                            from (select id
                                from countries
                                where (id = addresses.country_id) limit 1) x) as country
                        from addresses
                        where (user_id = users.id) limit 1) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    # #####################
    # # many
    # #####################


    def test_link_many
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.many :address, fk: "user_id = users.id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select id
                        from addresses
                        where (user_id = users.id)) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_many_filter
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.many :address, fk: "user_id = users.id", filter: "id % 2 = 0"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select id
                        from addresses
                        where (user_id = users.id) and (id % 2 = 0)) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_many_order_by
      res = to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
        s.root :user
        s.type :user, fields: [:id, :email] do |t|
          t.many :address, fk: "user_id = users.id", order_by: "id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select id
                        from addresses
                        where (user_id = users.id) order by id desc) x) as address
                from users
                where id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

  end
end