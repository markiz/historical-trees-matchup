require_relative "shared"
require_relative "test_runner"

require "active_record"
require "activerecord-import"

END_OF_TIME = 10**7

class Version < ActiveRecord::Base
  def self.active_at(timestamp)
    where("timestamp < ?", timestamp).order(timestamp: :desc).first
  end

  def self.current
    @current ||= snapshot(0)
  end

  def self.snapshot(timestamp)
    previous_version = active_at(timestamp)&.id || 0

    new_version = create!(timestamp: timestamp)
    @current = new_version if timestamp > previous_version

    rows = Node
             .active_at(timestamp, previous_version)
             .group(:key)
             .having("valid_since = MAX(valid_since)")
             .pluck(:key, :parent_key, :valid_since, :valid_until)
    columns = [:key, :parent_key, :valid_since, :valid_until, :version]
    new_rows = rows.map { |row| [*row, new_version.id] }
    Node.import columns, new_rows, validate: false

    new_version
  end
end

class Node < ActiveRecord::Base
  scope :active_at, -> (timestamp, version = Version.active_at(timestamp).id) {
    where("valid_since < ? AND valid_until >= ?", timestamp, timestamp)
      .where(version: version)
  }

  scope :with_key, -> (key) {
    where(key: key)
  }

  scope :with_parent_key, -> (parent_key) {
    where(parent_key: parent_key)
  }

  scope :for_version, -> (version) {
    where(version: version)
  }

  def self.add_node(timestamp, key, parent_key, valid_until: END_OF_TIME, version: nil)
    unless version
      # When and how often to snapshot is a big question that you will need to answer
      # depending on the needs of your app.
      @last_snapshot_timestamp ||= 0
      version = if timestamp - @last_snapshot_timestamp > SNAPSHOT_THRESHOLD
        @last_snapshot_timestamp = timestamp
        Version.snapshot(timestamp).id
      else
        Version.current.id
      end
    end

    create!(
      key: key,
      parent_key: parent_key,
      valid_since: timestamp,
      valid_until: valid_until,
      version: version
    )
  end

  def self.implode_node(timestamp, key)
    node = active_at(timestamp).with_key(key).first
    children = active_at(timestamp).with_parent_key(key)

    columns = [:key, :parent_key, :valid_since, :valid_until, :version]
    new_values = children.map { |c| [c.key, node.parent_key, timestamp, c.valid_until, node.version] }

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
      add_node(timestamp, key, new_parent_key, valid_until: node.valid_until, version: node.version)
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

unless ARGV.count >= 2 && ARGV.count <= 3
  $stderr.puts "USAGE: #{$0} <database-url> <data-file-name> [<snapshot-frequency>]"
  exit(1)
end

ActiveRecord::Base.establish_connection(ARGV[0])
ActiveRecord::Base.logger = Logger.new($stderr)
ActiveRecord::Base.logger.level = Logger::INFO

ActiveRecord::Schema.define do
  # Drop artifacts from other test runs if you have them
  drop_table :node_closures if table_exists?(:node_closures)

  create_table :versions, force: true do |t|
    t.integer :timestamp, null: false
    t.index :timestamp
  end

  create_table :nodes, force: true do |t|
    t.integer :key, null: false
    t.integer :parent_key
    t.integer :valid_since, null: false
    t.integer :valid_until, null: false
    t.integer :version, null: false

    t.index [:version, :key, :valid_since]
    t.index [:version, :parent_key, :valid_since]
  end
end

SNAPSHOT_THRESHOLD = (ARGV[2] || 5000).to_i # make a snapshot every XX ticks

TestRunner.new(:parent_id_snapshots, ARGV[0], ARGV[1], Node).run
