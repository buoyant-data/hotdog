#!/usr/bin/env ruby
#
# This script is useful for generating some load to send to hotdog, best way to
# run it is with:
#   ruby generate-stdout.rb | openssl s_client -connect localhost:6514
#

require 'json'

count = ARGV.first  || 100_000

count.to_i.times do |count|
  x = {
    :meta => {
      :topic => :test,
    },
    :i => count,
  }
  puts "<13>1 2020-04-18T15:16:09.956153-07:00 coconut tyler - - - #{x.to_json}"
  $stderr.write('.')
  $stdout.flush
end

