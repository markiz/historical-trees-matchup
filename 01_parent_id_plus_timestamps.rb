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

  scope :with_parent_key, -> (parent_key) {
    where(parent_key: parent_key)
  }

  def self.add_node(timestamp, key, parent_key, valid_until: END_OF_TIME)
    create!(
      key: key,
      parent_key: parent_key,
      valid_since: timestamp,
      valid_until: valid_until
    )
  end

  def self.implode_node(timestamp, key)
    node = active_at(timestamp).with_key(key).first
    children = active_at(timestamp).with_parent_key(key)

    columns = [:key, :parent_key, :valid_since, :valid_until]
    new_values = children.map { |c| [c.key, node.parent_key, timestamp, c.valid_until] }

    transaction(isolation: :read_committed) do
      node.update!(valid_until: timestamp)
      children.update_all(valid_until: timestamp)
      import columns, new_values, validate: false
    end
  end

  def self.change_parent(timestamp, key, old_parent_key, new_parent_key)
    node = active_at(timestamp).with_key(key).first
    raise ArgumentError, "mismatching parent key" unless old_parent_key == node.parent_key

    transaction(isolation: :read_committed) do
      add_node(timestamp, key, new_parent_key, valid_until: node.valid_until)
      node.update!(valid_until: timestamp)
    end
  end

  def self.ancestors(timestamp, key)
    result = []
    current_key = key
    loop do
      node = active_at(timestamp).with_key(current_key).first
      break unless node && node.parent_key

      result << node.parent_key
      current_key = node.parent_key
    end
    result
  end

  def self.descendants(timestamp, key)
    result = []
    current_children = [key]
    loop do
      current_children = active_at(timestamp).with_parent_key(current_children).pluck(:key)
      break unless current_children.any?
      result.concat(current_children)
    end
    result
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
  # Drop artifacts from other test runs if you have them
  drop_table :node_closures if table_exists?(:node_closures)
  drop_table :versions if table_exists?(:versions)

  create_table :nodes, force: true do |t|
    t.integer :key, null: false
    t.integer :parent_key
    t.integer :valid_since, null: false
    t.integer :valid_until, null: false

    t.index [:key]
    t.index [:parent_key]
  end
end

TestRunner.new(:parent_id_plus_timestamps, ARGV[0], ARGV[1], Node).run
