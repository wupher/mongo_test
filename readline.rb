#!/usr/bin/ruby
require "rubygems"
require "mongo"



#从日志中读取记录并转换成hash
def read_log_file(filename)
  result = []
  File.readlines(filename).each do |line|
    contents = line.split("\003")
    track = {:device_no  => contents[0], :sim => contents[1], :type => contents[2].to_i, :GPS_time => time_trans2(contents[3]),
             :valid => contents[4], :loc => {:long => contents[5].to_f, :lat => contents[6].to_f}, :altitude => contents[7].to_f,
             :speed => contents[8].to_f, :navigation_course => contents[9].to_f, :KM => contents[10].to_f, :parameter => contents[11].to_f,
             :receive_time => time_trans2(contents[12])} if contents.size == 13
    result << track if track
  end
  result
end

def get_conn(coll="track_log")
  db = Mongo::Connection.new("localhost").db('test')
  db.collection(coll) 
end

#将记录保存在mongdb中
def save_to_mongo(tracks)
  coll = get_conn
  # tracks.each{ |trac| coll.insert trac  }  #单条插入
  coll.insert tracks #批量插入
end

def search_with_field_value(search)
  coll = get_conn
  coll.find(search).each{ |row| p row  }
end

#返回表中的记录数
def collections_count()
  get_conn.count
end

#查询一条记录
def find_a_record(collection)
  get_conn.find_one
end

def list_dir(dir_path)
  result = []
  Dir.foreach(dir_path){ |path| result << "#{dir_path}/#{path}" if path!='.' && path!=".." && path!='.DS_Store'  }
  result
end

def insert_data_test()
  logs_files = list_dir('/Users/fanwu/Desktop/trck')
  logs_files.each do |log|
    data = read_log_file(log)
    save_to_mongo(data)
  end
  puts "insert #{collections_count} data into table"
end

def do_with_data()
  logs_files = list_dir('/Users/fanwu/Desktop/trck')
  logs_files.each each {|log| yield read_log_file(log)}
  puts "insert #{collections_count} data into table"
end


def time_trans2(time_str)
  m = /(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)/.match(time_str.strip)
  Time.local(m[1], m[2], m[3], m[4], m[5], m[6]).utc if m
end

def add_index(field)
  get_conn.create_index(field)
end


# 50.times{insert_data_test}

coll = get_conn
# coll.create_index([["loc", Mongo::GEO2D]])
# add_index('type')
# add_index('device_no')
# p coll.count
# p coll.find("type" => {'$ne'  => '002'}).count
results = coll.find({"loc" => {"$near" => [110,26], "$maxDistance" => 5}})

results.each{ |row| p row  }
p results.count()
# p coll.count

