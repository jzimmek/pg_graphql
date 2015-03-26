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
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users
                where users.id = 1 limit 1) x) as value      
      SQL
      ), token(res)

      # ---

      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users
                where users.id = 1 limit 1) x) as value      
      SQL
      ), token(res)

      # ---

      res = to_sql({user: {email: "email"}}) do |s|
        s.root :user
        s.type :user, null_pk: :array, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users.id,
                  email
                from users) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_fail_when_accessing_non_root
      assert_raise_message ":user is not a root type" do
        res = to_sql({user: {id: 1, email: "email"}}) do |s|
          s.type :user, fields: [:email]
        end
      end
    end

    def test_simple_fail_when_pass_id_field
      assert_raise_message "do not add :id in fields; it will be added automatically" do
        res = to_sql({user: {id: 1, email: "email"}}) do |s|
          s.root :user
          s.type :user, fields: [:id, :email]
        end
      end
    end

    def test_simple_fail_without_pk
      assert_raise_message "missing :id for root type :user" do
        res = to_sql({user: {email: "email"}}) do |s|
          s.root :user
          s.type :user, fields: [:email]
        end
      end

      assert_raise_message "found empty :id array on type :user" do
        res = to_sql({user: {id: [], email: "email"}}) do |s|
          s.root :user
          s.type :user, fields: [:email]
        end
      end
    end

    def test_simple_pk_custom
      res = to_sql({user: {id: "1", email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email], pk: ->(id, level){ "access_token = '#{id}'" }
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users where access_token = '1' limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_simple_pk_with_level
      res = to_sql({user: {id: "99", email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email], pk: ->(id, level){ "level#{level} = '#{id}'" }
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users where level1 = '99' limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_simple_pk_type_handling
      res = to_sql({user: {id: ["1"], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users.id,
                  email
                from users where users.id in ('1')) x) as value      
      SQL
      ), token(res)

      # ---

      res = to_sql({user: {id: "1", email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users where users.id = '1' limit 1) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_pk_array_one
      res = to_sql({user: {id: [1], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users.id,
                  email
                from users where users.id in (1)) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_pk_array_multiple
      res = to_sql({user: {id: [1,2], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users.id,
                  email
                from users where users.id in (1,2)) x) as value      
      SQL
      ), token(res)

      # ---

      res = to_sql({user: {id: ['1','2'], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users.id,
                  email
                from users where users.id in ('1','2')) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_filter
      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email], filter: "id > 100"
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users
                where users.id = 1 and (id > 100) limit 1) x) as value      
      SQL
      ), token(res)
    end

    def test_simple_multiple
      res = to_sql({user: {id: 1, email: "email"}, educator: {id: 99}}) do |s|
        s.root :user
        s.root :educator
        s.type :user, fields: [:email]
        s.type :educator, null_pk: true
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email
                from users
                where users.id = 1 limit 1) x) as value
        union all
        select 'educator'::text as key,
          (select to_json(x.*)
            from (select educators.id
                from educators where educators.id = 99 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_simple_strange_nested_to_json_for_json_datatype_with_column_alias
      # fails: select 'flow'::text as key, (select to_json(x.*) from (select id, data from flows where id = '1' limit 1) x) as value;
      # fine: select 'flow'::text as key, (select to_json(x.*) from (select id, to_json(data) from flows where id = '1' limit 1) x) as value;

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data}]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows.id, data from flows where flows.id = 1 limit 1) x) as value
      SQL
      ), token(res)

      # ------

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data, as: nil}]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows.id, data from flows where flows.id = 1 limit 1) x) as value
      SQL
      ), token(res)

      # ------

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data, expr: ->(c){ "to_json(#{c})" } }]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows.id, to_json(data) as data from flows where flows.id = 1 limit 1) x) as value
      SQL
      ), token(res)

      # ------ positive check

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data, as: nil, expr: ->(c){ "to_json(#{c})" } }]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows.id, to_json(data) from flows where flows.id = 1 limit 1) x) as value
      SQL
      ), token(res)

    end

    #####################
    # inherit
    #####################

    def test_inherit
      res = to_sql({
        products: {
          type: nil,
          clickout__destination_url: nil,
          download__download_url: nil,
          download__users: {
            orders: {}
          }
        }
      }) do |s|
        s.root :product

        s.type :user, fields: [:email] do |t|
          t.many :orders, fk: "user_id = users.id"
        end

        s.type :order

        s.type :product, null_pk: :array, fields: [:type, :clickout__destination_url, :download__download_url] do |t|

          t.subtype :download, table: :product_downloads, fk: "download.id = products.id and products.type = 'download'"
          t.subtype :clickout, table: :product_clickouts, fk: "clickout.id = products.id and products.type = 'clickout'"

          t.many :download__users, type: :user, fk: "id = download.id"
        end

      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select products.id,
                  type,
                  clickout.destination_url as clickout__destination_url,
                  download.download_url as download__download_url,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select users.id,
                          (select to_json(coalesce(json_agg(x.*), '[]'::json))
                            from (select orders.id
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

    def test_inherit_with_pk
      res = to_sql({
        products: {
          id: 1,
          clickout__destination_url: nil
        }
      }) do |s|
        s.root :product
        s.type :product, null_pk: :array, fields: [:clickout__destination_url, :download__download_url] do |t|
          t.subtype :clickout, table: :product_clickouts, fk: "clickout.id = products.id and products.type = 'clickout'"
        end
      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(x.*)
            from (select products.id, type,
                  clickout.destination_url as clickout__destination_url
                from products
                left join product_clickouts as clickout on (clickout.id = products.id
                    and products.type = 'clickout') where products.id = 1 limit 1) x) as value
      SQL
      ), token(res)
      
    end

    #####################
    # one
    #####################


    def test_link_one
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "id = users.address_id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (id = users.address_id) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_nested_pk
      res = to_sql({user: {id: 1, email: "email", address: {id: 99}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "id = users.address_id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where addresses.id = 99 and (id = users.address_id) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)

      res = to_sql({user: {id: 1, email: "email", address: {id: [99,999]}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "id = users.address_id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where addresses.id in (99,999) and (id = users.address_id) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)    
    end

    def test_link_one_empty_fields
      res = to_sql({user: {id: 1, email: "email", address: {}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "id = users.address_id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where (id = users.address_id) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_missing_fk
      assert_raise_message "missing :fk on link :address" do
        to_sql({user: {id: 1, email: "email", address: {id: "id"}}}) do |s|
          s.root :user
          s.type :user, fields: [:email] do |t|
            t.one :address
          end
          s.type :address
        end
      end      
    end


    def test_link_one_fk_sql
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "id = (select 100)"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (id = (select 100)) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_filter
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "user_id = users.id", filter: "id > 100"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (user_id = users.id) and (id > 100) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_one_order_by
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.one :address, fk: "user_id = users.id", order_by: "id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (user_id = users.id) order by id desc limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    #####################
    # one-in-one
    #####################

    def test_link_one_in_one
      res = to_sql({user: {id: 1, email: "email", address: {country: {}}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
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
            from (select users.id,
                  email,
                  (select to_json(x.*)
                    from (select addresses.id,
                          (select to_json(x.*)
                            from (select countries.id
                                from countries
                                where (id = addresses.country_id) limit 1) x) as country
                        from addresses
                        where (user_id = users.id) limit 1) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    # #####################
    # # many
    # #####################


    def test_link_many
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, fk: "user_id = users.id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (user_id = users.id)) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_many_nested_pk
      res = to_sql({user: {id: 1, email: "email", address: {id: ["99","999"]}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, fk: "user_id = users.id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses.id
                        from addresses
                        where addresses.id in ('99','999') and (user_id = users.id)) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_many_empty_fields
      res = to_sql({user: {id: 1, email: "email", address: {}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, fk: "user_id = users.id"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses.id
                        from addresses
                        where (user_id = users.id)) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_many_filter
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, fk: "user_id = users.id", filter: "id % 2 = 0"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (user_id = users.id) and (id % 2 = 0)) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

    def test_link_many_order_by
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, fk: "user_id = users.id", order_by: "id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users.id,
                  email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses.id
                        from addresses
                        where addresses.id = '99' and (user_id = users.id) order by id desc) x) as address
                from users
                where users.id = 1 limit 1) x) as value
      SQL
      ), token(res)
    end

  end
end