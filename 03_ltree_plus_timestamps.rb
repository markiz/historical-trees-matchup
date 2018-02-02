require_relative "shared"
require_relative "test_runner"

require "active_record"
require "activerecord-import"

END_OF_TIME = 10**7

class Node < ActiveRecord::Base
  scope :active_at, -> (timestamp) {
    where("valid_since < ? AND valid_until >= ?", timestamp, timestamp)
  }

  scope :with_key, -> (key) {
    where(key: key)
  }

  scope :descendants_of, -> (path) {
    where("path <@ ? and path != ?", path, path)
  }

  def self.add_node(timestamp, key, parent_key, valid_until: END_OF_TIME)
    parent_node = active_at(timestamp).with_key(parent_key).first
    path = parent_node ? "#{parent_node.path}.#{key}" : key.to_s

    create!(
      key: key,
      path: path,
      valid_since: timestamp,
      valid_until: valid_until
    )
  end

  def self.implode_node(timestamp, key)
    node = active_at(timestamp).with_key(key).first
    descendants = active_at(timestamp).descendants_of(node.path)

    new_path_prefix = node.parent_prefix
    columns = [:key, :path, :valid_since, :valid_until]
    new_values = descendants.map { |d| [d.key, d.path.sub(/(\.|^)#{key}\./, "\\1"), timestamp, d.valid_until] }

    transaction(isolation: :read_committed) do
      node.update!(valid_until: timestamp)
      descendants.update_all(valid_until: timestamp)
      import columns, new_values, validate: false
    end
  end

  def self.change_parent(timestamp, key, old_parent_key, new_parent_key)
    node = active_at(timestamp).with_key(key).first
    raise ArgumentError, "mismatching_parent" unless node.parent_key == old_parent_key
    descendants = active_at(timestamp).descendants_of(node.path)
    new_parent = active_at(timestamp).with_key(new_parent_key).first

    transaction(isolation: :read_committed) do
      new_node = add_node(timestamp, key, new_parent_key, valid_until: node.valid_until)
      columns = [:key, :path, :valid_since, :valid_until]
      new_values = descendants.map do |d|
        [d.key, d.path.sub(/^#{Regexp.escape(node.path)}/, new_node.path), timestamp, d.valid_until]
      end

      descendants.update_all(valid_until: timestamp)
      import columns, new_values, validate: false
      node.update!(valid_until: timestamp)
    end
  end

  def self.ancestors(timestamp, key)
    node = active_at(timestamp).with_key(key).first
    node.path.split(".").reverse[1..-1].map(&:to_i)
  end

  def self.descendants(timestamp, key)
    node = active_at(timestamp).with_key(key).first
    active_at(timestamp).descendants_of(node.path).pluck(:key)
  end

  def parent_prefix
    path.sub(/\.[^.]+$/, "")
  end

  def parent_key
    match = path.match(/\.?([^.]+)\.[^.]+$/)
    match[1].to_i if match
  end
end


unless ARGV.count == 2
  $stderr.puts "USAGE: #{$0} <database-url> <data-file-name>"
  exit(1)
end


ActiveRecord::Base.establish_connection(ARGV[0])
ActiveRecord::Base.logger = Logger.new($stderr)
ActiveRecord::Base.logger.level = Logger::INFO

ActiveRecord::Schema.define do
  enable_extension :ltree

  # Drop artifacts from other test runs if you have them
  drop_table :node_closures if table_exists?(:node_closures)
  drop_table :versions if table_exists?(:versions)

  create_table :nodes, force: true do |t|
    t.integer :key, null: false
    t.column :path, :ltree, null: false
    t.integer :valid_since, null: false
    t.integer :valid_until, null: false

    t.index [:key]
    t.index [:path], using: :gist
  end
end

TestRunner.new(:ltree_timestamps, ARGV[0], ARGV[1], Node).run
