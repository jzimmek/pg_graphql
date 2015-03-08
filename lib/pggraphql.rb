require "active_support/all"
require "pggraphql/version"

module PgGraphQl

  class Schema
    attr_reader :types, :roots
    def initialize
      @types = {}
      @roots = []
      yield(self) if block_given?

      @roots = @roots.map{|r| @types[r]}
    end

    def root(name)
      @roots << name
    end

    def type(name, opts={})
      type = @types[name] = Type.new(self, name)
      opts.each_pair{|key, val| type.send(:"#{key}=", val) }
      yield(type) if block_given?
    end

    def to_sql(query, level=0, parent=nil, link_name=nil)
      if level > 0
        query.map do |e|
          link = parent ? parent.links[link_name] : nil
          type = link ? link.type : self.types[e[0].to_s.singularize.to_sym]
          ids = e[1][:id]

          columns = e[1].map do |f|
            column_name = f[0]

            raise "unknown field #{column_name.inspect} on type #{type.name.inspect}" if !f[1].is_a?(Hash) && !type.fields.include?(column_name)
            raise "unknown link #{column_name.inspect} on type #{type.name.inspect}" if f[1].is_a?(Hash) && !type.links.include?(column_name)

            (f[1].is_a?(Hash) ? "(" + to_sql([f].to_h, level + 1, type, column_name) + ") as #{column_name}" : column_name.to_s)
          end.join(",")

          is_many = (link && link.many?) || !ids || ids.is_a?(Array)
          order_by = link.try(:order_by) || type.try(:order_by)

          wheres = []

          if ids && ids.to_s != "id"
            if ids.is_a?(Array)
              wheres << "id in (#{ids.join(',')})" unless ids.empty?
            else
              wheres << "id = #{ids}"
            end
          end

          wheres << ("(" + type.filter + ")") if type.filter

          if link
            wheres << if link.invert
              "id = #{parent.table}.#{link.fk}"
            else
              "#{link.fk} = #{parent.table}.id"
            end

            wheres << ("(" + link.filter + ")") if link.filter
          end

          sql = "select to_json("
          sql += "coalesce(json_agg(" if is_many
          sql += "x.*"
          sql += "), '[]'::json)" if is_many
          sql += ") from (select #{columns} from #{type.table}"
          sql += " where #{wheres.join(' and ')}" unless wheres.empty?
          sql += " order by #{order_by}" if order_by
          sql += " limit 1" if !is_many
          sql += ") x"

        end.join
      else
        wrap_root(query.map do |e|
          sql = to_sql([e].to_h, 1)
          "select '#{e[0]}'::text as key, (#{sql}) as value"
        end.join("\nunion all\n"))
      end
    end

    def wrap_root(sql)
      "select ('{'||string_agg(to_json(t1.key)||':'||coalesce(to_json(t1.value), 'null'), ',')||'}')::json res from (\n" + sql + ") t1"
    end

    class Type
      attr_accessor :name, :table, :filter, :links, :order_by, :fields
      attr_reader :schema
      def initialize(schema, name)
        @schema = schema
        @name = name
        @table = name.to_s.pluralize.to_sym
        @fields = [:id]
        @filter = nil
        @order_by = nil
        @links = {}
      end
      def one(name, opts={})
        create_link(name, false, opts)
      end
      def many(name, opts={})
        create_link(name, true, opts)
      end
      def create_link(name, many, opts)
        link = @links[name] = Link.new(self, name, many)
        opts.each_pair do |key, val| 
          link.send(:"#{key}=", val)
        end
        link
      end
    end

    class Link
      attr_accessor :name, :invert, :filter, :order_by
      def initialize(owner, name, many)
        @owner = owner
        @name = name
        @many = many
        @invert = false
        @order_by = nil
      end
      def fk=(_fk)
        @_fk = fk
      end
      def fk
        @invert ? "#{@name.to_s.singularize}_id" : "#{@owner.name}_id"
      end
      def type=(_type)
        @_type = _type
      end
      def type
        @owner.schema.types[@_type||@name.to_s.singularize.to_sym]
      end
      def many?
        !!@many
      end
    end

  end


end
