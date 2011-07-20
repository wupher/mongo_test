#!/usr/bin/env ruby
require "./track_gen"
require "mongo"
require "thread"

$conn = Mongo::Connection.new('localhost').db('realtime_tracks').collection('car_tracks')


def update_track
  6666.times do
    track = generate_track_log_by_calculation(rand(200))
    track["_id"] = track[:device_no]
    $conn.update({"_id" => track["_id"]},track,{:upsert => true})
  end
end


  while true
    stamp = Time.now;
    update_track;
    used_time = Time.now - stamp
    gap = 60 - (used_time)
    puts "in time: #{gap}, used time: #{used_time}" if gap > 0
    puts "out of time: #{gap}" if gap < 0
  end
