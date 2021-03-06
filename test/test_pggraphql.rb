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

      res = s.to_sql(query)

      sql, params = res.values_at(:sql, :params)
      puts sql if print_sql

      res
    end

    def test_simple_level_aware
      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      assert_equal [1], res[:params]
    end

    def test_nested_level_aware
      res = to_sql({user: {id: 1, email: "email", address: {}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.has_one :address, order_by: "{users:root}.id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where (addresses2.user_id = users1.id) order by users1.id desc limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      assert_equal [1], res[:params]

      # ---


      res = to_sql({user: {id: 1, email: "email", address: {person: {}}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.has_one :address
        end
        s.type :address do |t|
          t.has_one :person
        end

        s.type :person, order_by: "{users:closest}.id desc"
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id,
                        (select to_json(x.*)
                          from (select people3.id
                              from people as people3
                              where (people3.address_id = addresses2.id) order by users1.id desc limit 1) x) as "person"
                        from addresses as addresses2
                        where (addresses2.user_id = users1.id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      assert_equal [1], res[:params]

    end


    def test_simple
      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      assert_equal [1], res[:params]

      # ---

      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

      # ---

      res = to_sql({user: {email: "email"}}) do |s|
        s.root :user
        s.type :user, null_pk: :array, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users1.id,
                  users1.email as email
                from users as users1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [], res[:params]

      # ---

      res = to_sql({user: {email: nil, other: nil, :"$custom" => {name: "bob"}}}) do |s|
        s.root :user
        s.type :user, null_pk: :array, fields: [:email, {name: :other, expr: ->(c, query){ "'#{query[:"$custom"][:name]}'" }}]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users1.id,
                  users1.email as email,
                  'bob' as other
                from users as users1) x) as value
      SQL
      ), token(res[:sql])

    end

    def test_simple_igore_dollar_fields
      res = to_sql({user: {id: 1, email: nil, :"$custom" => nil}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
    end

    def test_simple_table_query
      res = to_sql({user: {id: 1, email: nil}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.table_query = "select 1 as id, 'my@domain' as email"
        end
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from (select 1 as id, 'my@domain' as email) as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      # ----

      res = to_sql({user: {id: 1, email: nil}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.table_query = "select 1 as id, 'my@domain' as email where {users}.id > 0"
        end
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from (select 1 as id, 'my@domain' as email where users1.id > 0) as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      # ----

      res = to_sql({user: {id: 1, email: nil, :"$custom" => {order: "email"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.table_query = ->(query) do
            "select 1 as id, 'my@domain' as email order by #{query[:"$custom"][:order]} desc"
          end
        end
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from (select 1 as id, 'my@domain' as email order by email desc) as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
    end

    def test_simple_table_query_with_params
      res = to_sql({user: {id: 1, email: nil}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.table_query = ["select ? as id, 'my@domain' as email", 99]
        end
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from (select ? as id, 'my@domain' as email) as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      assert_equal [99, 1], res[:params]
    end

    def test_guard_field
      res = to_sql({user: {id: 1, email: nil}}) do |s|
        s.root :user
        s.type :user, fields: [{name: :email, guard: "true"}]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  case
                    when true then users1.email
                    else null
                  end as email
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
    end

    def test_guard_link_belongs_to
      res = to_sql({user: {id: 1, email: nil, address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :address, guard: "true"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  case
                    when true then (select to_json(x.*)
                      from (select addresses2.id
                          from addresses as addresses2
                          where addresses2.id = ? and (addresses2.id = users1.address_id) limit 1) x)
                    else null
                  end as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_guard_link_many
      res = to_sql({user: {id: 1, email: nil, address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, guard: "true"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  case
                    when true then (select to_json(coalesce(json_agg(x.*), '[]'::json))
                      from (select addresses2.id
                          from addresses as addresses2
                          where addresses2.id = ? and (addresses2.user_id = users1.id)) x)
                    else to_json('[]'::json)
                  end as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
    end

    def test_guard_link_many_with_fk
      res = to_sql({user: {id: 1, email: nil, address: {id: 99}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, guard: ["2 = ?", 2]
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  case
                    when 2 = ? then (select to_json(coalesce(json_agg(x.*), '[]'::json))
                      from (select addresses2.id
                          from addresses as addresses2
                          where addresses2.id = ? and (addresses2.user_id = users1.id)) x)
                    else to_json('[]'::json)
                  end as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])

      assert_equal [2, 99, 1], res[:params]
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
        s.type :user, fields: [:email], pk: ->(id, level){ ["access_token = ?", id] }
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1 where access_token = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["1"], res[:params]
    end

    def test_simple_pk_with_level
      res = to_sql({user: {id: "99", email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email], pk: ->(id, level){ ["level#{level} = ?", id] }
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1 where level1 = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99"], res[:params]
    end

    def test_simple_pk_type_handling
      res = to_sql({user: {id: ["1"], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users1.id,
                  users1.email as email
                from users as users1 where users1.id in ?) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [["1"]], res[:params]

      # ---

      res = to_sql({user: {id: "1", email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1 where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["1"], res[:params]
    end

    def test_simple_pk_array_one
      res = to_sql({user: {id: [1], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users1.id,
                  users1.email as email
                from users as users1 where users1.id in ?) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [[1]], res[:params]
    end

    def test_simple_pk_array_multiple
      res = to_sql({user: {id: [1,2], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users1.id,
                  users1.email as email
                from users as users1 where users1.id in ?) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [[1,2]], res[:params]

      # ---

      res = to_sql({user: {id: ['1','2'], email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email]
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select users1.id,
                  users1.email as email
                from users as users1 where users1.id in ?) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [["1","2"]], res[:params]
    end

    def test_simple_filter
      res = to_sql({user: {id: 1, email: "email"}}) do |s|
        s.root :user
        s.type :user, fields: [:email], filter: "id > 100"
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email
                from users as users1
                where users1.id = ? and (id > 100) limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]
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
            from (select users1.id,
                  users1.email as email
                from users as users1
                where users1.id = ? limit 1) x) as value
        union all
        select 'educator'::text as key,
          (select to_json(x.*)
            from (select educators1.id
                from educators as educators1 where educators1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1, 99], res[:params]
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
        select 'flow'::text as key, (select to_json(x.*) from (select flows1.id, flows1.data as data from flows as flows1 where flows1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

      # ------

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data, as: nil}]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows1.id, flows1.data from flows as flows1 where flows1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

      # ------

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data, expr: ->(c, query){ "to_json(#{c})" } }]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows1.id, to_json(flows1.data) as data from flows as flows1 where flows1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

      # ------ positive check

      res = to_sql({flow: {id: 1, data: nil}}) do |s|
        s.root :flow
        s.type :flow do |t|
          t.fields = [{name: :data, as: nil, expr: ->(c, query){ "to_json(#{c})" } }]
        end
      end

      assert_equal token(<<-SQL
        select 'flow'::text as key, (select to_json(x.*) from (select flows1.id, to_json(flows1.data) from flows as flows1 where flows1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

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
          t.many :orders
        end

        s.type :order
        s.type :product, null_pk: :array, fields: [:type, :clickout__destination_url, :download__download_url] do |t|
          t.subtype :clickout, table: :product_clickouts
          t.subtype :download, table: :product_downloads do |st|
            st.many :users, type: :user
          end
        end
      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select products1.id,
                  products1.type as type,
                  clickout1.destination_url as clickout__destination_url,
                  download1.download_url as download__download_url,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select users2.id,
                          (select to_json(coalesce(json_agg(x.*), '[]'::json))
                            from (select orders3.id
                                from orders as orders3
                                where (orders3.user_id = users2.id)) x) as "orders"
                        from users as users2
                        where (users2.product_id = download1.id)) x) as "download__users"
                from products as products1
                left join product_clickouts as clickout1 on (clickout1.id = products1.id
                    and products1.type = 'clickout')
                left join product_downloads as download1 on (download1.id = products1.id
                    and products1.type = 'download')) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [], res[:params]

      # ------

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
          t.many :orders
        end

        s.type :order
        s.type :product, null_pk: :array, fields: [:type, :clickout__destination_url, :download__download_url] do |t|
          t.subtype :download, table: :product_downloads do |st|
            st.many :users, type: :user
          end
          t.subtype :clickout, table: :product_clickouts
        end
      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(coalesce(json_agg(x.*), '[]'::json))
            from (select products1.id,
                  products1.type as type,
                  clickout1.destination_url as clickout__destination_url,
                  download1.download_url as download__download_url,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select users2.id,
                          (select to_json(coalesce(json_agg(x.*), '[]'::json))
                            from (select orders3.id
                                from orders as orders3
                                where (orders3.user_id = users2.id)) x) as "orders"
                        from users as users2
                        where (users2.product_id = download1.id)) x) as "download__users"
                from products as products1
                left join product_downloads as download1 on (download1.id = products1.id
                    and products1.type = 'download')
                left join product_clickouts as clickout1 on (clickout1.id = products1.id
                    and products1.type = 'clickout')) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [], res[:params]
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
          t.subtype :clickout, table: :product_clickouts
        end
      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(x.*)
            from (select products1.id,
                  products1.type as type,
                  clickout1.destination_url as clickout__destination_url
                from products as products1
                left join product_clickouts as clickout1 on (clickout1.id = products1.id
                    and products1.type = 'clickout') where products1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

    end

    def test_inherit_with_table_query
      res = to_sql({
        products: {
          id: 1,
          clickout__destination_url: nil
        }
      }) do |s|
        s.root :product
        s.type :product, null_pk: :array, fields: [:clickout__destination_url] do |t|
          t.subtype :clickout, table: :product_clickouts do |st|
            st.table_query = <<-SQL
              select 1 as id, 'someurl' as destination_url
            SQL
          end
        end
      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(x.*)
            from (select products1.id,
                  products1.type as type,
                  clickout1.destination_url as clickout__destination_url
                from products as products1
                left join (select 1 as id, 'someurl' as destination_url) as clickout1 on (clickout1.id = products1.id
                    and products1.type = 'clickout') where products1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]

      # ----

      res = to_sql({
        products: {
          id: 1,
          clickout__destination_url: nil
        }
      }) do |s|
        s.root :product
        s.type :product, null_pk: :array, fields: [:clickout__destination_url] do |t|
          t.subtype :clickout, table: :product_clickouts do |st|
            st.table_query = ["select 1 as id, ? as destination_url", "someurl"]
          end
        end
      end

      assert_equal token(<<-SQL
        select 'products'::text as key,
          (select to_json(x.*)
            from (select products1.id,
                  products1.type as type,
                  clickout1.destination_url as clickout__destination_url
                from products as products1
                left join (select 1 as id, ? as destination_url) as clickout1 on (clickout1.id = products1.id
                    and products1.type = 'clickout') where products1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["someurl", 1], res[:params]
    end

    #####################
    # one
    #####################

    def test_link_belongs_to
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.id = users1.address_id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_belongs_to___name_different_from_type
      res = to_sql({user: {id: 1, email: "email", other_address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :other_address, type: :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.id = users1.other_address_id) limit 1) x) as "other_address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_has_one___name_different_from_type
      res = to_sql({user: {id: 1, email: "email", other_address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.has_one :other_address, type: :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id) limit 1) x) as "other_address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_one
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.id = users1.address_id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_one_nested_pk
      res = to_sql({user: {id: 1, email: "email", address: {id: 99}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.id = users1.address_id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [99, 1], res[:params]

      res = to_sql({user: {id: 1, email: "email", address: {id: [99,999]}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id in ? and (addresses2.id = users1.address_id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [[99,999], 1], res[:params]
    end

    def test_link_one_empty_fields
      res = to_sql({user: {id: 1, email: "email", address: {}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.belongs_to :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where (addresses2.id = users1.address_id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]
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
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (id = (select 100)) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_one_filter
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.has_one :address, filter: "id > 100"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id) and (id > 100) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_one_order_by
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.has_one :address, order_by: "id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id) order by id desc limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    # #####################
    # # one-in-one
    # #####################

    def test_link_one_in_one
      res = to_sql({user: {id: 1, email: "email", address: {country: {}}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.has_one :address
        end
        s.type :address do |t|
          t.belongs_to :country
        end
        s.type :country
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(x.*)
                    from (select addresses2.id,
                          (select to_json(x.*)
                            from (select countries3.id
                                from countries as countries3
                                where (countries3.id = addresses2.country_id) limit 1) x) as "country"
                        from addresses as addresses2
                        where (addresses2.user_id = users1.id) limit 1) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]
    end

    # #####################
    # # many
    # #####################


    def test_link_many
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id)) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]

      # ----

      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id)) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]

      # ----

      res = to_sql({user: {id: 1, email: "email", other_addresses: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :other_addresses, type: :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id)) x) as "other_addresses"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_many_nested_pk
      res = to_sql({user: {id: 1, email: "email", address: {id: ["99","999"]}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id in ? and (addresses2.user_id = users1.id)) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [["99","999"], 1], res[:params]
    end

    def test_link_many_empty_fields
      res = to_sql({user: {id: 1, email: "email", address: {}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where (addresses2.user_id = users1.id)) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal [1], res[:params]
    end

    def test_link_many_filter
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, filter: "id % 2 = 0"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id) and (id % 2 = 0)) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_link_many_order_by
      res = to_sql({user: {id: 1, email: "email", address: {id: "99"}}}) do |s|
        s.root :user
        s.type :user, fields: [:email] do |t|
          t.many :address, order_by: "id desc"
        end
        s.type :address
      end

      assert_equal token(<<-SQL
        select 'user'::text as key,
          (select to_json(x.*)
            from (select users1.id,
                  users1.email as email,
                  (select to_json(coalesce(json_agg(x.*), '[]'::json))
                    from (select addresses2.id
                        from addresses as addresses2
                        where addresses2.id = ? and (addresses2.user_id = users1.id) order by id desc) x) as "address"
                from users as users1
                where users1.id = ? limit 1) x) as value
      SQL
      ), token(res[:sql])
      assert_equal ["99", 1], res[:params]
    end

    def test_handle_sql_part
      s = PgGraphQl::Schema.new

      params = []
      level = 1
      table_levels={}

      assert_equal "users1.id = 1", s.handle_sql_part("{users}.id = 1", params, level, table_levels)
      assert_equal "users2.id = 2", s.handle_sql_part("{users}.id = 2", params, level + 1, table_levels)
      assert_equal "users1.id = 3", s.handle_sql_part("{users}.id = 3", params, level, table_levels)

      assert_equal "users1.id = ?", s.handle_sql_part(["{users}.id = ?", 101], params, level, table_levels)
      assert_equal "users2.id = ?", s.handle_sql_part(["{users}.id = ?", 102], params, level + 1, table_levels)
      assert_equal "users1.id = ?", s.handle_sql_part(["{users}.id = ?", 103], params, level, table_levels)

      assert_equal "users1.id = ?", s.handle_sql_part(["{users}.id = ?", 101], params, level, table_levels)
      assert_equal "educators2.id = ?", s.handle_sql_part(["{educators}.id = ?", 102], params, level + 1, table_levels)
      assert_equal "users1.id = ?", s.handle_sql_part(["{users}.id = ?", 103], params, level, table_levels)
      assert_equal "educators1.id = ?", s.handle_sql_part(["{educators}.id = ?", 104], params, level, table_levels)
    end

  end
end