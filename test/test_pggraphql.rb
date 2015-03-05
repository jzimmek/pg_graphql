require "test-unit"
require "pggraphql"

module PgGraphQl
  class PgGraphQlTest < ::Test::Unit::TestCase

    def token(sql)
      sql = sql.gsub(/\n/, " ").gsub("=", " = ").gsub(/,/, " , ").gsub("(", " ( ").gsub(")", " ) ").gsub(/[ ]+/, " ")
      sql.strip.split(" ").map{|e| e.strip}.reject{|e| e.empty?}
    end

    def query(*args, &block)
      Schema.new(&block).query(*args)
    end

    # schema

    def test_schema_root_type
      assert_equal :sometype, Schema.new{|e| e.root(:sometype)}.roots[:sometype][:type]
      assert_equal :person, Schema.new{|e| e.root(:people)}.roots[:people][:type]
    end

    # type

    def test_pk_prefixed_by_as
      assert_equal :"sometypes.id", Type.new(:sometype).pk!
    end

    # type - defaults

    def test_type_default_fields
      assert_equal [:id], Type.new(:sometype).fields
    end

    def test_type_default_as
      assert_equal :sometypes, Type.new(:sometype).as
      assert_equal :people, Type.new(:person).as
    end

    def test_type_default_sql
      assert_equal "select * from houses", Type.new(:house).sql
      assert_equal "select * from people", Type.new(:person).sql
    end

    def test_type_default_pk
      assert_equal :id, Type.new(:sometype).pk
    end

    def test_type_link_default_fk
      assert_equal :sometype_id, Type.new(:sometype).tap{|e| e.one(:somelink) }.links[:somelink][1][:fk]
      assert_equal :sometype_id, Type.new(:sometype).tap{|e| e.many(:somelink) }.links[:somelink][1][:fk]
      assert_equal :other_fk, Type.new(:sometype).tap{|e| e.one(:somelink, fk: :other_fk) }.links[:somelink][1][:fk]
      assert_equal :other_fk, Type.new(:sometype).tap{|e| e.many(:somelink, fk: :other_fk) }.links[:somelink][1][:fk]
    end

    def test_type_link_default_type
      assert_equal :somelink, Type.new(:sometype).tap{|e| e.one(:somelink) }.links[:somelink][1][:type]
      assert_equal :somelink, Type.new(:sometype).tap{|e| e.many(:somelink) }.links[:somelink][1][:type]

      assert_equal :person, Type.new(:sometype).tap{|e| e.one(:people) }.links[:people][1][:type]
      assert_equal :person, Type.new(:sometype).tap{|e| e.many(:people) }.links[:people][1][:type]

      assert_equal :other_type, Type.new(:sometype).tap{|e| e.one(:somelink, type: :other_type) }.links[:somelink][1][:type]
      assert_equal :other_type, Type.new(:sometype).tap{|e| e.many(:somelink, type: :other_type) }.links[:somelink][1][:type]
    end

    def test_unknown_field
      assert_raise_message "unknown field" do
        query([[:user, 1], :id, :name]) do |s|
          s.root :user
          s.type :user
        end
      end
    end

    def test_simple
      res = query([[:user, 1], :id, :name]) do |s|
        s.root :user
        s.type :user, fields: [:id, :name]
      end

      assert_equal token("select to_json(x.*) res from (select id, name from users where id in (1) limit 1) x"), token(res)
    end

    def test_simple_filter
      res = query([[:user, 1], :id, :name]) do |s|
        s.root :user
        s.type :user, fields: [:id, :name], filter: "id % 2 = 0"
      end

      assert_equal token("select to_json(x.*) res from (select id, name from users where id in (1) and id % 2 = 0 limit 1) x"), token(res)

      res = query([[:user, 1], :id, :name]) do |s|
        s.root :user
        s.type :user, fields: [:id, :name], filter: "id % 2 = 0 and id > 10"
      end

      assert_equal token("select to_json(x.*) res from (select id, name from users where id in (1) and id % 2 = 0 and id > 10 limit 1) x"), token(res)
    end

    def test_one
      res = query([[:user, 1], :id, [[:address], :id]]) do |s|
        s.root :user
        s.type :user do |t|
          t.one :address
        end
        s.type :address
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select 
            id, 
            (select to_json(x.*) res from (select id from addresses where user_id = users.id limit 1) x) as address 
          from users 
          where id in (1)
          limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    def test_one_fk_invert
      res = query([[:user, 1], :id, [[:address], :id]]) do |s|
        s.root :user
        s.type :user do |t|
          t.one :address, fk: :address_id, invert: true
        end
        s.type :address
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select 
            id, 
            (select to_json(x.*) res from (select id from addresses where id = users.address_id limit 1) x) as address 
          from users 
          where id in (1)
          limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    # def test_one_polymorph
    #   res = query([[:user, 1], :id, [[:address], :id]]) do |s|
    #     s.root :user
    #     s.type :user do |t|
    #       t.one :address, polymorph: [:primary, :secondary]
    #     end
    #     s.type :address
    #     s.type :address_primary
    #     s.type :address_secondary
    #   end

    #   expected = <<-SQL
    #     select 
    #       to_json(x.*) res 
    #     from (
    #       select 
    #         id, 
    #         (select to_json(x.*) res from (
    #           select 'primary' as type, id from addresses join addresses_primary using (id) where user_id = users.id and addresses.type = 'primary'
    #           union
    #           select 'secondary' as type, id from addresses join addresses_secondary using (id) where user_id = users.id and addresses.type = 'secondary'
    #         ) x) as address 
    #       from users 
    #       where id in (1)
    #       limit 1
    #     ) x
    #   SQL

    #   assert_equal token(expected), token(res)
    # end

    def test_one_filter
      res = query([[:user, 1], :id, [[:address], :id]]) do |s|
        s.root :user
        s.type :user do |t|
          t.one :address, filter: "is_default = true"
        end
        s.type :address
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select 
            id, 
            (select to_json(x.*) res from (select id from addresses where user_id = users.id and is_default = true limit 1) x) as address 
          from users 
          where id in (1)
          limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    def test_one_fk_through
      res = query([[:user, 1], :id, [[:address], :id]]) do |s|
        s.root :user
        s.type :user do |t|
          t.one :address, fk: ->(l) do
            "select id from some_address_table where user_id = #{t.pk!}"
          end
        end
        s.type :address
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select 
            id, 
            (select to_json(x.*) res from (select id from addresses where id in (select id from some_address_table where user_id = users.id) limit 1) x) as address 
          from users 
          where id in (1)
          limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    def test_one_in_one
      res = query([[:user, 1], :id, [[:address], :id, [[:person], :id]]]) do |s|
        s.root :user
        s.type :user do |t|
          t.one :address
        end
        s.type :address do |t|
          t.one :person
        end
        s.type :person
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select 
            id, 
            (select to_json(x.*) res from (
              select 
                id,
                (select to_json(x.*) res from (select id from people where address_id = addresses.id limit 1) x) as person
              from addresses where user_id = users.id limit 1
            ) x) as address 
          from users 
          where id in (1) 
          limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    def test_many
      res = query([[:user, 1], :id, [[:addresses], :id]]) do |s|
        s.root :user
        s.type :user do |t|
          t.many :addresses
        end
        s.type :address
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select id, 
          (select to_json(coalesce(json_agg(x.*), '[]'::json)) res from (select id from addresses where user_id = users.id) x) as addresses 
          from users 
          where id in (1) limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    def test_many_with_ids
      res = query([[:user, 1], :id, [[:addresses, 100], :id]]) do |s|
        s.root :user
        s.type :user do |t|
          t.many :addresses
        end
        s.type :address
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select id, 
          (select to_json(coalesce(json_agg(x.*), '[]'::json)) res from (select id from addresses where user_id = users.id and id in (100)) x) as addresses 
          from users 
          where id in (1) limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end

    def test_one_in_many
      res = query([[:user, 1], :id, [[:addresses], :id, [[:person], :id]]]) do |s|
        s.root :user
        s.type :user do |t|
          t.many :addresses
        end
        s.type :address do |t|
          t.one :person
        end
        s.type :person
      end

      expected = <<-SQL
        select 
          to_json(x.*) res 
        from (
          select 
            id, 
            (select to_json(coalesce(json_agg(x.*), '[]'::json)) res from (
              select 
                id,
                (select to_json(x.*) res from (select id from people where address_id = addresses.id limit 1) x) as person
              from addresses where user_id = users.id
            ) x) as addresses 
          from users 
          where id in (1) 
          limit 1
        ) x
      SQL

      assert_equal token(expected), token(res)
    end
  end
end