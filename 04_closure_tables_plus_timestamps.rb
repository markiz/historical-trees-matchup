require_relative "shared"
require_relative "test_runner"

require "active_record"
require "activerecord-import"

END_OF_TIME = 10**7

class NodeClosure < ActiveRecord::Base
  scope :active_at, -> (timestamp) {
    where("valid_since < ? AND valid_until >= ?", timestamp, timestamp)
  }

  scope :for_ancestor, -> (key) {
    where(ancestor: key)
  }

  scope :for_descendant, -> (key) {
    where(descendant: key)
  }
end

class Node < ActiveRecord::Base
  def self.add_node(timestamp, key, parent_key, valid_until: END_OF_TIME)
    columns = [:ancestor, :descendant, :level, :valid_since, :valid_until]
    new_closures = NodeClosure
                    .for_descendant(parent_key)
                    .active_at(timestamp)
                    .pluck(:ancestor, :level, :valid_until)
                    .map do |ancestor, level, valid_until|
                      [ancestor, key, level + 1, timestamp, valid_until]
                    end
    new_closures += [[parent_key, key, 1, timestamp, valid_until]] if parent_key

    transaction(isolation: :read_committed) do
      create!(key: key)
      NodeClosure.import columns, new_closures, validate: false
    end
  end

  def self.implode_node(timestamp, key)
    # Let's say we have a tree of
    # 1 -> 2 -> 3, 4
    # And we are imploding node 2
    # Then ancestors are closures (1->2 level 1)
    # And descendants are closures (2->3 level 1 and 2->4 level 1)
    # And descendants_for_ancestors are closures (1->3 level 2 and 1->4 level 2)
    # We need to update valid_until for ancestors and descendants
    # And we need to insert new descendants_for_ancestors with level = level - 1
    ancestors = NodeClosure
                  .for_descendant(key)
                  .active_at(timestamp)

    descendants = NodeClosure
                    .for_ancestor(key)
                    .active_at(timestamp)

    descendants_for_ancestors = NodeClosure
                                  .for_ancestor(ancestors.pluck(:ancestor))
                                  .for_descendant(descendants.pluck(:descendant))
                                  .active_at(timestamp)

    columns = [:ancestor, :descendant, :level, :valid_since, :valid_until]
    new_descendants_for_ancestors = descendants_for_ancestors.pluck(*columns).map do |ancestor, descendant, level, valid_since, valid_until|
      [ancestor, descendant, level - 1, timestamp, valid_until]
    end

    transaction(isolation: :read_committed) do
      ancestors.update_all(valid_until: timestamp)
      descendants.update_all(valid_until: timestamp)
      descendants_for_ancestors.update_all(valid_until: timestamp)
      NodeClosure.import(columns, new_descendants_for_ancestors, validate: false)
    end
  end

  def self.change_parent(timestamp, key, old_parent_key, new_parent_key)
    # Let's say we have a tree of
    # 1 -> 2 -> 3, 4
    # 1 -> 5
    # And we are changing parent of (2) from (1) to (5)
    # Then ancestors are closures (1->2 level 1)
    # And descendants are closures (2->3 level 1 and 2->4 level 1)
    # And descendants_for_ancestors are closures (1->3 level 2 and 1->4 level 2)
    # And ancestors_for_new_parent are closures (1->5 level 1)
    #
    # We need to update valid_until for ancestors, descendants and descendants_for_ancestors
    # And we need to insert new closures for each descendant for each new parent with level = descendant_level + parent_ancestor_level
    # And new closures for each new parent for the switching node
    ancestors = NodeClosure
                  .for_descendant(key)
                  .active_at(timestamp)

    descendants = NodeClosure
                    .for_ancestor(key)
                    .active_at(timestamp)

    old_descendant_ancestors = NodeClosure
                                 .for_descendant(descendants.pluck(:descendant))
                                 .for_ancestor(ancestors.pluck(:ancestor))
                                 .active_at(timestamp)

    new_parent_ancestors = NodeClosure
                             .for_descendant(new_parent_key)
                             .active_at(timestamp)

    columns = [:ancestor, :descendant, :level, :valid_since, :valid_until]
    descendant_data = descendants.pluck(*columns)
    new_closures = new_parent_ancestors.pluck(*columns).flat_map do |a_ancestor, a_descendant, a_level, a_valid_since, a_valid_until|
      [[a_ancestor, key, a_level + 1, timestamp, END_OF_TIME]] +
        descendant_data.map do |d_ancestor, d_descendant, d_level, d_valid_since, d_valid_until|
          [a_ancestor, d_descendant, a_level + d_level + 1, timestamp, END_OF_TIME]
        end
    end
    new_closures.concat [[new_parent_key, key, 1, timestamp, END_OF_TIME]]
    new_closures.concat descendant_data.map { |ancestor, descendant, level, valid_since, valid_until| [new_parent_key, descendant, level + 1, timestamp, END_OF_TIME] }

    transaction(isolation: :read_committed) do
      ancestors.update_all(valid_until: timestamp)
      old_descendant_ancestors.update_all(valid_until: timestamp)
      NodeClosure.import(columns, new_closures, validate: false)
    end
  end

  def self.ancestors(timestamp, key)
    NodeClosure
      .for_descendant(key)
      .active_at(timestamp)
      .order(:level)
      .pluck(:ancestor)
  end

  def self.descendants(timestamp, key)
    NodeClosure
      .for_ancestor(key)
      .active_at(timestamp)
      .pluck(:descendant)
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
  drop_table :versions if table_exists?(:versions)

  create_table :nodes, force: true do |t|
    t.integer :key, null: false
  end

  create_table :node_closures, force: true do |t|
    t.integer :ancestor, null: false
    t.integer :descendant, null: false
    t.integer :level, null: false
    t.integer :valid_since, null: false
    t.integer :valid_until, null: false

    t.index [:ancestor]
    t.index [:descendant]
  end
end

TestRunner.new(:closure_tables, ARGV[0], ARGV[1], Node).run
