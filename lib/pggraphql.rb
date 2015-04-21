require "active_support/all"
require "sequel"
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

    def handle_sql_part(part, params)
      if part.is_a?(Array)
        part.slice(1..-1).each{|param| params << param}
        part[0]
      elsif part.is_a?(String)
        part
      else
        raise "unsupported sql part: #{part}"
      end
    end

    def to_sql(query, level=0, params=[], parent=nil, link_name=nil)
      if level > 0
        query.map do |e|
          link = parent ? parent.links[link_name.to_s.split("@").first.to_sym] : nil
          type = link ? link.type : self.types[e[0].to_s.split("@").first.singularize.to_sym]
          ids = e[1][:id]

          raise "type not found: #{e[0]}; link_name: #{link_name}" unless type

          raise "found :id with without value on type #{type.name.inspect}" if e[1].key?(:id) && ids.nil?
          raise "found empty :id array on type #{type.name.inspect}" if e[1].key?(:id) && ids.is_a?(Array) && ids.empty?


          raise "#{type.name.inspect} is not a root type" if level == 1 && !@roots.include?(type)
          raise "missing :fk on link #{link.name.inspect}" if link && !link.fk

          requested_fields = {id: nil} # always add :id field
          requested_fields = requested_fields.merge(type: nil) unless type.subtypes.empty? # always add :subtype
          requested_fields = requested_fields.merge(e[1])

          columns = requested_fields.map do |f|
            nested_link_name = f[0]
            field_name = f[0]

            raise "unknown field #{field_name.inspect} on type #{type.name.inspect}" if !f[1].is_a?(Hash) && !type.fields.detect{|f| f[:name] == field_name}
            raise "unknown link #{field_name.inspect} on type #{type.name.inspect}" if f[1].is_a?(Hash) && !type.links.include?(field_name.to_s.split("@").first.to_sym)

            if f[1].is_a?(Hash)
              "(" + to_sql([f].to_h, level + 1, params, type, nested_link_name) + ") as \"#{field_name}\""
            else
              field_def = type.fields.detect{|f| f[:name] == field_name}

              column_name = field_def[:name].to_s.index("__") ? field_def[:name].to_s.gsub(/__/, ".").to_sym : field_def[:name]
              # column_expr = type.mappings[field_name] || column_name

              column_expr = if field_def[:expr]
                handle_sql_part(field_def[:expr].call(column_name), params)
              else
                column_name
              end

              if (column_name == field_name && column_name == column_expr)
                column_name.to_s
              else
                "#{column_expr}" + (field_def[:as] ? " as #{field_def[:as]}" : "")
              end
            end

          end.join(",")

          is_many = (link && link.many?) || (level == 1 && ids.is_a?(Array)) || (level == 1 && !ids && type.null_pk == :array)
          order_by = link.try(:order_by) || type.try(:order_by)

          wheres = []

          raise "missing :id for root type #{type.name.inspect}" if !ids && level == 1 && !type.null_pk

          if ids && type.pk.call(ids, level)
            wheres << handle_sql_part(type.pk.call(ids, level), params)
          end

          wheres << ("(" + handle_sql_part(type.filter, params) + ")") if type.filter

          if link
            fk = link.fk.is_a?(Proc) ? link.fk.call(level) : link.fk

            if link_name.to_s.index("__")
              subtype_type, subtype_link_name = link_name.to_s.split("__")

              fk = "#{link.type.table}.id = #{subtype_type}.#{subtype_link_name}_id" if fk == :belongs_to
              fk = "#{link.type.table}.#{parent.name}_id = #{subtype_type}.id" if fk == :has_one
              fk = "#{link.type.table}.#{parent.name}_id = #{subtype_type}.id" if fk == :many
            else
              fk = "#{link.type.table}.id = #{parent.table}.#{link.name}_id" if fk == :belongs_to
              fk = "#{link.type.table}.#{parent.name}_id = #{parent.table}.id" if fk == :has_one
              fk = "#{link.type.table}.#{parent.name}_id = #{parent.table}.id" if fk == :many
            end

            wheres << ("(" + handle_sql_part(fk, params) + ")")
            wheres << ("(" + handle_sql_part(link.filter, params) + ")") if link.filter
          end

          sql = "select to_json("
          sql += "coalesce(json_agg(" if is_many
          sql += "x.*"
          sql += "), '[]'::json)" if is_many
          sql += ") from (select #{columns} from #{type.table}"
        
          unless type.subtypes.empty?
            sql += "\n" + type.subtypes.map do |f|
              subtype = f[1]
              fk = subtype.fk.is_a?(Proc) ? subtype.fk.call(level) : subtype.fk

              fk = "#{subtype.name}.id = #{type.table}.id and #{type.table}.type = '#{subtype.name}'" if fk == :subtype

              subtype_as = subtype.fk.is_a?(Proc) ? "#{subtype.name}#{level}" : subtype.name
              # subtype_as = (link && parent && link.type == parent.name) ? "#{subtype.name}#{level}" : subtype.name
              "left join #{subtype.table} as #{subtype_as} on (#{handle_sql_part(fk, params)})"
            end.join("\n")
          end


          sql += " where #{wheres.join(' and ')}" unless wheres.empty?
          sql += " order by #{order_by}" if order_by
          sql += " limit 1" if !is_many
          sql += ") x"

        end.join
      else
        root_sql = wrap_root(query.map do |e|          
          sql = to_sql([e].to_h, 1, params)
          "select '#{e[0]}'::text as key, (#{sql}) as value"
        end.join("\nunion all\n"))

        {sql: root_sql, params: params}
      end
    end

    def wrap_root(sql)
      "select ('{'||string_agg(to_json(t1.key)||':'||coalesce(to_json(t1.value), 'null'), ',')||'}')::json res from (\n" + sql + ") t1"
    end

    class Type
      attr_accessor :name, :table, :links, :order_by, :filter, :subtypes, :pk, :null_pk
      attr_reader :schema, :mappings, :fields
      def initialize(schema, name)
        @schema = schema
        @name = name
        @table = name.to_s.pluralize.to_sym
        @fields = []
        @filter = nil
        @order_by = nil
        @links = {}
        @subtypes = {}
        @null_pk = false
        @pk = ->(ids, level) do
          id_column = "#{@table}.id"
          if ids.is_a?(Array)
            # "#{id_column} in (" + ids.map{|id| id.is_a?(String) ? "'#{id}'" : id.to_s}.join(',') + ")"
            ["#{id_column} in ?", ids]
          else
            # "#{id_column} = " + (ids.is_a?(String) ? "'#{ids}'" : "#{ids}")
            ["#{id_column} = ?", ids]
          end
        end
      end
      def fields=(fields)
        fields.each do |f|
          raise "do not add :id in fields; it will be added automatically" if f == :id || (f.is_a?(Hash) && f[:name] == :id)
        end
        @fields = fields.map{|f| create_field(f)}
      end
      def fields
        @fields + [create_field({name: :id, as: nil, expr: ->(c){ "#{@table}.#{c}" }})] + (@subtypes.empty? ? [] : [create_field(:type)])
      end
      def create_field(field)
        if field.is_a?(Symbol)
          {name: field, as: field}
        elsif field.is_a?(Hash)
          raise "missing field :name #{field.inspect}" unless field[:name]
          field[:as] = field[:name] unless field.key?(:as)
          field
        else
          raise "unsupported field #{field.inspect}"
        end         
      end

      def one(name, opts={})
        create_link(name, false, opts)
      end

      def has_one(name, opts={})
        one(name, opts.merge({fk: :has_one}))
      end

      def belongs_to(name, opts={})
        one(name, opts.merge({fk: :belongs_to}))
      end

      def many(name, opts={})
        create_link(name, true, {fk: :many}.merge(opts))
      end
      def subtype(name, opts={})
        subtype = @subtypes[name] = SubType.new(self, name)
        {fk: :subtype}.merge(opts).each_pair do |key, val| 
          subtype.send(:"#{key}=", val)
        end
        yield(subtype) if block_given?
        subtype
      end
      def create_link(name, many, opts)
        link = @links[name] = Link.new(self, name, many)
        opts.each_pair do |key, val| 
          link.send(:"#{key}=", val)
        end
        link
      end
    end

    class SubType
      attr_accessor :name, :table, :fk
      attr_reader :type
      def initialize(type, name)
        @type = type
        @name = name
        @table = nil
        @fk = nil
      end
      def has_one(name, opts={})
        @type.has_one(:"#{@name}__#{name}", {type: name}.merge(opts))
      end
      def belongs_to(name, opts={})
        @type.belongs_to(:"#{@name}__#{name}", {type: name}.merge(opts))
      end
      def one(name, opts={})
        @type.one(:"#{@name}__#{name}", {type: name}.merge(opts))
      end
      def many(name, opts={})
        @type.many(:"#{@name}__#{name}", {type: name}.merge(opts))
      end
    end

    class Link
      attr_accessor :name, :filter, :fk, :order_by
      def initialize(owner, name, many)
        @owner = owner
        @name = name
        @many = many
        @order_by = nil
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
