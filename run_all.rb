require "yaml"

test_runs = YAML.load(File.read("test_runs.yml"))
test_runs.each do |test_run|
  test_run.fetch("databases").each do |db|
    test_run.fetch("datasets").each do |dataset|
      puts "RUNNING #{[RbConfig.ruby, test_run["file"], db, dataset, *test_run["extra_args"]&.map(&:to_s)].join(" ")}"
      pid = spawn RbConfig.ruby, test_run["file"], db, dataset, *test_run["extra_args"]&.map(&:to_s)
      Process.wait pid
    end
  end
end
