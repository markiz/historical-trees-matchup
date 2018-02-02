require_relative "shared"

class Node
  attr_reader :key, :parent, :children

  def initialize(key, parent = nil)
    @key = key
    @parent = parent
    @children = []
  end

  def add_child(node)
    children << node
  end

  def remove_child(node)
    children.delete(node)
  end

  def change_parent(new_parent, no_remove = false)
    parent.remove_child(self) if parent && !no_remove
    new_parent.add_child(self) if new_parent
    @parent = new_parent
  end

  # removes itself from the tree and moves all children to the parent
  def implode
    parent.remove_child(self) if parent
    children.each { |c| c.change_parent(parent, true) }
    children.clear
  end

  def descendants
    [*children, *children.flat_map(&:descendants)]
  end

  def ancestors
    [*parent, *parent&.ancestors]
  end
end


class Generator
  attr_reader :timestamp, :all_nodes, :events, :tests, :next_node_key,
              :updates, :reads_per_update, :initial_inserts

  def initialize(updates: 100, reads_per_update: 10, initial_inserts: 50)
    @updates = updates
    @reads_per_update = reads_per_update
    @initial_inserts = initial_inserts
  end

  def generate
    reset_state

    initial_inserts.times do
      @timestamp += 1

      event_add_node
    end

    updates.times do
      @timestamp += 1

      case rand(100)
      when 0..85
        event_add_node
      when 86..95
        event_change_parent
      when 95..99
        event_implode_node
      end

      reads_per_update.times do
        @timestamp += 1

        case rand(100)
        when 0..80
          test_ancestors
        when 81..99
          test_descendants
        end
      end
    end

    {
      events: events,
      tests: tests,
      seed: Random::DEFAULT.seed
    }
  end

  private

  def reset_state
    @timestamp = 0
    @next_node_key = 0
    @all_nodes = []
    @events = []
    @tests = []
  end

  def event_add_node
    @next_node_key += 1

    parent = all_nodes.sample
    node = Node.new(next_node_key, parent)
    parent.add_child(node) if parent

    all_nodes << node
    events << Event.new(timestamp, :add_node, { key: node.key, parent: parent&.key })
  end

  def event_implode_node(node = nil)
    node ||= all_nodes.sample
    cc = node.children.count
    node.implode
    all_nodes.delete(node)

    events << Event.new(timestamp, :implode_node, { key: node.key })
  end

  def event_change_parent
    # we only want nodes with a parent having at least one other child
    # for this test.
    # also must not be a circular dep
    node = nil
    new_parent = nil
    i = 0

    loop do
      # This could technically loop infinitely (albeit with low probability),
      # so we're adding a counter to prevent that
      i += 1
      return if i > 1000

      node = all_nodes.sample

      # no circular references allowed
      descendants = node.descendants
      suitable_parents = all_nodes - descendants - [node]
      new_parent = suitable_parents.sample

      break if new_parent
    end

    old_parent = node.parent
    node.change_parent(new_parent)
    events << Event.new(timestamp, :change_parent, { key: node.key, old_parent: old_parent&.key, new_parent: new_parent.key })
  end

  def test_ancestors
    node = all_nodes.sample
    test_result = node.ancestors

    tests << Test.new(timestamp, :ancestors, { key: node.key }, test_result.map(&:key))
  end

  def test_descendants
    node = all_nodes.sample
    test_result = node.descendants

    tests << Test.new(timestamp, :descendants, { key: node.key }, test_result.map(&:key))
  end
end

opts = Slop.parse do |o|
  o.on "-h", "--help", "Print this message" do
    puts o
    exit(1)
  end
  o.integer "--seed", "Random seed", default: Random.new_seed
  o.string "-o", "--output-file", "Target file for output"
  o.bool "--console", "Open console prompt after generation"
  o.integer "--initial-inserts", "Number of initial insertions", default: 50
  o.integer "--update-num", "Number of total tree updates", default: 100
  o.integer "--reads-per-update", "Reads per update", default: 10
end

unless opts[:output_file] || opts[:console]
  puts opts
  puts "Either --output-file or --console is required"
  exit(1)
end

srand(opts[:seed])
generator = Generator.new(
  updates: opts[:update_num],
  reads_per_update: opts[:reads_per_update],
  initial_inserts: opts[:initial_inserts]
)
result = generator.generate
if opts[:console]
  binding.pry
else
  File.write(opts[:output_file], Marshal.dump(result))
end
