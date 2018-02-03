require "yaml"

DATABASES = ["mysql", "postgresql"]
SETS = ["small_set.dat", "medium_set.dat", "large_set.dat"]
METRICS = ["add_node", "change_parent", "implode_node", "ancestors", "descendants"]
TOTALS = ["setup_total", "tests_total", "db_size"]

results = YAML.load(File.read("test_results.yml"))
METHODS = results.keys

rows = []

DATABASES.each do |db|
  methods = METHODS.select { |m| results[m].key?(db) }
  rows << ["db", db]
  METRICS.each do |metric|
    rows << [metric, *methods]
    SETS.each do |set|
      rows << [set, *methods.map { |m| results[m][db][set]["metrics"][metric]["time_per_call"] }]
    end
  end

  TOTALS.each do |total|
    rows << [total, *methods]

    SETS.each do |set|
      rows << [set, *methods.map { |m| results[m][db][set][total] }]
    end
  end
end

File.write("test_results.csv", rows.map { |row| row.join("\t") }.join("\n"))
