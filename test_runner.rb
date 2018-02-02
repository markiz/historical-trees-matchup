require "yaml"
require "active_support/all"

class TestRunner
  attr_reader :method_name, :db_url, :data_file, :data, :test_class
  attr_reader :metrics, :db_size
  def initialize(method_name, db_url, data_file, test_class)
    @method_name = method_name
    @db_url = db_url
    @data_file = data_file
    @data = Marshal.load(File.read(data_file))
    @test_class = test_class
  end

  def run
    @metrics = {}

    instrumented(:setup_total) do
      data[:events].each do |e|
        case e.type
        when :add_node
          instrumented(:add_node) do
            test_class.add_node(
              e.timestamp,
              e.arguments.fetch(:key),
              e.arguments.fetch(:parent)
            )
          end
        when :implode_node
          instrumented(:implode_node) do
            test_class.implode_node(
              e.timestamp,
              e.arguments.fetch(:key)
            )
          end
        when :change_parent
          instrumented(:change_parent) do
            test_class.change_parent(
              e.timestamp,
              e.arguments.fetch(:key),
              e.arguments.fetch(:old_parent),
              e.arguments.fetch(:new_parent)
            )
          end
        end
      end
    end

    instrumented(:tests_total) do
      data[:tests].each do |t|
        case t.type
        when :ancestors
          result = instrumented(:ancestors) { test_class.ancestors(t.timestamp, t.arguments[:key]) }

          raise "ancestors test failed" unless result == t.result
        when :descendants
          result = instrumented(:descendants) { test_class.descendants(t.timestamp, t.arguments[:key]) }

          # descendants are allowed to be unsorted
          binding.pry unless result.sort == t.result.sort
          raise "descendants test failed" unless result.sort == t.result.sort
        end
      end
    end

    @db_size = calculate_db_size
    dump_results
  end

  private

  def calculate_db_size
    if db_type == "mysql"
      ActiveRecord::Base
        .connection
        .execute(
          <<-SQL
          SELECT SUM(data_length + index_length) / 1024.0 / 1024 AS size
          FROM information_schema.tables
          WHERE table_schema='#{ActiveRecord::Base.connection.current_database}'
          GROUP BY table_schema
          SQL
        ).to_a[0][0].to_f
    else
      ActiveRecord::Base
        .connection
        .execute(
          <<-SQL
          SELECT pg_database_size('#{ActiveRecord::Base.connection.current_database}') / 1024.0 / 1024 as size
          SQL
        ).to_a[0]["size"].to_f
    end
  end

  def dump_results
    all_results = if File.exist?("test_results.yml")
      YAML.load(File.read("test_results.yml"))
    else
      {}
    end
    setup_total = metrics.delete(:setup_total)[:time_per_call]
    tests_total = metrics.delete(:tests_total)[:time_per_call]
    test_label = [method_name, db_type, ].join("/")

    result = {
      method_name => {
        db_type => {
          File.basename(data_file) => {
            metrics: metrics,
            setup_total: setup_total,
            tests_total: tests_total,
            db_size: db_size
          }
        }
      }
    }.deep_stringify_keys
    File.write("test_results.yml", YAML.dump(all_results.deep_merge(result)))
  end

  def db_type
    case db_url
    when /mysql/
      "mysql"
    when /pg/, /postgresql/
      "postgresql"
    else
      raise ArgumentError, "unknown db type"
    end
  end

  def instrumented(label, &block)
    result = nil
    time = Benchmark.realtime { result = block.call }
    metrics[label] ||= { calls: 0, total_time: 0 }
    metrics[label][:calls] += 1
    metrics[label][:total_time] += time
    metrics[label][:time_per_call] = metrics[label][:total_time] / metrics[label][:calls]
    result
  end
end
