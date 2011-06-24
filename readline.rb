#!/usr/bin/ruby
require "rubygems"
require "mongo"

module MongoHelper
  def init_db_conn(coll="track_log")
    Mongo::Connection.new(@ip,@port).db(@name).collection(coll)
  end
  
  def save(data)
    @conn.insert data
  end
  
  def search(query)
    @conn.find(query)
  end
  
  def count()
    @conn.count()
  end
  
  def get_a_random_data()
    @conn.find_one
  end
  
  def add_index(field)
    @conn.create_index(field)
  end
  
  def add_geo_index()
    @conn.create_index([["loc", Mongo::GEO2D]])
  end
end


module LogHelper 
  def read_data(log_file)
    result = []
    File.readlines(log_file).each do |line|
      contents = line.split("\003")
      track = {:device_no  => (contents[0][3..-1]).to_i, :sim => contents[1], :type => contents[2].to_i, :GPS_time => time_trans1(contents[3]),
               :valid => contents[4], :loc => {:long => contents[5].to_f, :lat => contents[6].to_f}, :altitude => contents[7].to_f,
               :speed => contents[8].to_f, :navigation_course => contents[9].to_f, :KM => contents[10].to_f, :parameter => contents[11].to_f,
               :receive_time => time_trans2(contents[12])} if contents.size == 13
      result << track if track
    end
    result
  end
  
  def time_trans1(time_str)
    m = /(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)/.match(time_str.strip)
    Time.gm(m[1], m[2], m[3], m[4], m[5], m[6]).utc if m
  end
  
  def time_trans2(time_str)
    m = /(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)/.match(time_str.strip)
    Time.local(m[1], m[2], m[3], m[4], m[5], m[6]).utc if m
  end
  
  def list_log_files(log_dir)
    result = []
    Dir.foreach(log_dir){ |path| result << "#{log_dir}/#{path}" if path!='.' && path!=".." && path!='.DS_Store'  }
    result
  end
end

class MongoTest
  include MongoHelper, LogHelper
  
  def initialize(log_dir, db_address='localhost', db_port='27017', db_name='test')
    @log_dir = log_dir
    @ip=db_address
    @port=db_port
    @name = db_name
    @log_files = list_log_files(@log_dir)
    @conn = init_db_conn
  end
  
  def get_conn(strategy="share")
    return @conn
  end
  
  def insert_data()
    @log_files.each{ |log| save(read_data(log))}
  end 
  
  def search_track(query)
    search(query)
  end
end

#__main__
test = MongoTest.new('/Users/fanwu/workspace/data/track')
puts "before insert: #{test.count} data"
test.insert_data
# test.add_index('device_no')
puts "after insert: #{test.count} data"

