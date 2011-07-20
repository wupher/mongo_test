#!/usr/bin/env ruby
require "rubygems"
require "mongo"

conn = Mongo::Connection.new('localhost').db('big_tracks').collection('big_tracks')
conn.find("device_no" => 18905918695).each{ |row| p row  }
