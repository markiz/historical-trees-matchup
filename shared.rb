require "bundler/setup"
require "pry"
require "benchmark"
require "slop"

Event = Struct.new(:timestamp, :type, :arguments)
Test = Struct.new(:timestamp, :type, :arguments, :result)

