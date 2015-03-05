require "json"
require "active_support/all"
require "pggraphql/version"

module PgGraphQl

  class Schema
    attr_accessor :roots

    def initialize
      @roots = {}
      @types = {}
      yield(self) if block_given?

      @types = @types.inject({}) do |memo, e|
        t = memo[e[0]] = Type.new(e[0])

        e[1].each_pair do |key, val|
          t.send("#{key}=", val) unless key == :block
        end

        e[1][:block].call(t) if e[1][:block]
        memo
      end
    end

    def root(name, opts={})
      opts[:type] = name.to_s.singularize.to_sym unless opts[:type]
      @roots[name] = opts
    end

    def type(name, opts={}, &block)
      opts[:block] = block
      @types[name] = opts
    end

    def query(query, level=0, parent_link=nil, parent_type=nil)
      requested_type = @types[query[0][0].to_sym]
      requested_ids = query[0][1].is_a?(Array) ? query[0][1] : [query[0][1]].reject{|e| !e}

      sql = requested_type.sql

      where_conditions = []

      if parent_link
        if parent_link[:fk].is_a?(Symbol)        
          where_conditions << (parent_link[:invert] ? " #{requested_type.pk} = #{parent_type.as}.#{parent_link[:fk]}" : " #{parent_link[:fk]} = #{parent_type.pk!}")
        elsif parent_link[:fk].is_a?(Proc)
          where_conditions << (" #{requested_type.pk} in (" + parent_link[:fk].call(parent_link) + ")")
        else
          raise "unsupported"
        end

        where_conditions << parent_link[:filter] if parent_link[:filter]
      end

      requested_columns = query[1..-1].map do |field|

        if field.is_a?(Symbol)
          raise "unknown field" unless requested_type.fields.include?(field)
          field.to_s
        elsif field.is_a?(Array)

          requested_link_field = field[0][0]
          requested_link = requested_type.links[requested_link_field][1]
          requested_link_ids = field[0][1]
          requested_link_type = @types[requested_link[:type]]

          requested_link_query = field.each_with_index.map do |e2, idx|
            idx == 0 ? [requested_link_type.name, requested_link_ids] : e2
          end

          "(" + self.query(requested_link_query, level + 1, requested_link, requested_type) + ") as #{requested_link_field}"
        else
          raise "unsupported"
        end

      end

      is_many = parent_type && parent_type.links[parent_link[:name]][0] == :many

      inner_sql = sql.gsub("*", requested_columns.join(", "))

      where_conditions << " #{requested_type.pk} in (#{requested_ids.map(&:to_s).join(',')})" unless requested_ids.empty?
      where_conditions << (" " + requested_type.filter) if requested_type.filter

      unless where_conditions.empty?
        inner_sql += " where"
        inner_sql += where_conditions.join(" and ")
      end

      inner_sql += (is_many ? "" : " limit 1")

      "select to_json(" + (is_many ? "coalesce(json_agg(x.*), '[]'::json)" : "x.*" ) + ") res from (#{inner_sql}) x"
    end

  end

  class Type
    attr_accessor :pk, :sql, :fields, :as, :filter
    attr_reader :links, :name
    def initialize(name)
      @name = name
      @links = {}
      @fields = [:id]
      @as = name.to_s.pluralize.to_sym
      @sql = "select * from #{name.to_s.pluralize}"
      @pk = :id
    end
    def pk!
      :"#{@as}.#{@pk}"
    end
    def one(name, opts={})
      opts[:fk] = :"#{@name}_id" unless opts[:fk]
      opts[:type] = name.to_s.singularize.to_sym unless opts[:type]
      opts[:name] = name
      @links[name] = [:one, opts]
    end
    def many(name, opts={})
      opts[:fk] = :"#{@name}_id" unless opts[:fk]
      opts[:type] = name.to_s.singularize.to_sym unless opts[:type]
      opts[:name] = name
      @links[name] = [:many, opts]
    end
  end
end
