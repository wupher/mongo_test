#!/usr/bin/env ruby
require "rubygems"
require "mongo"

#用来生成经纬度坐标列表，供将来生成随机的经纬度数据使用
#使用真实经纬度数据而非使用随机数的好处是，车辆在地图上画的时候不会出现在湖里面:-)
#经纬度坐标的数据空间是13万个，可能还有重复的，会出现一些数据重复的现象
def gen_loc_list
  conn_read = Mongo::Connection.new('localhost').db('int_test').collection('track_log')
  conn_write = Mongo::Connection.new('localhost').db('tracks').collection('loc')
  id = 0
  conn_read.find({},{:fields => ['loc']}).each do |row|
    conn_write.insert({"_id" => id, "loc" => {:long => row['loc']['long'], :lat => row['loc']['lat']}})
    id += 1
    puts id
  end
end

#用来生成电话号码列表，也是使用真实数据，其实也可以用随机数来生成
#真实的号码空间是6000来个，这样会超过6000以上的数据生成时就会有随机轨迹出现
def gen_phone_list
  conn_read = Mongo::Connection.new('localhost').db('int_test').collection('map_results')
  conn_write = Mongo::Connection.new('localhost').db('tracks').collection('phone_numbers')
  phone = []
  conn_read.find({},{:fields => "_id"}).each{ |row| phone << row["_id"]  }
  phone.each_with_index do |phone, i|
    conn_write.insert({"_id" => i, :phone => phone})
  end
end

#生成电话号码
def generate_phone_number(conn)
  phone = conn.find_one("_id" => (rand(conn.count())))
  phone['phone']
end

#生成经纬度坐标
def generate_gps_loc(conn)
  loc = conn.find_one("_id" => (rand(conn.count())))
  return generate_gps_loc(conn) if loc["loc"]["long"] == 0 or loc["loc"]["lat"] == 0
  {:long => loc["loc"]["long"], :lat => loc["loc"]["lat"]}
end

#生成轨迹数据
#time_seed 用于将当前时间向后推迟一段，这样不会出现轨迹时间相同或者密集的情况
def generate_track_log(time_seed, conn_phone, conn_loc)
  device_no = generate_phone_number(conn_phone)
  loc = generate_gps_loc(conn_loc)
  gps_time = Time.now + time_seed
  type = 2
  valid = "A"
  altitude = 38
  speed = rand(150)
  navigation_course = 0
  km = rand(10000)
  parameter = 1031
  recv_time = gps_time + 5
  {:device_no => device_no, :loc => loc, :GPS_time => gps_time.utc, :valid => valid,
    :altitude => altitude, :speed => speed, :navigation_course => navigation_course,
    :KM => km, :parameter => parameter, :recv_time => recv_time.utc, :type => type}
end

def gen_and_save_10Million_data()
  conn = Mongo::Connection.new('localhost').db('tracks').collection('test_tracks')
  conn_phone = Mongo::Connection.new('localhost').db('tracks').collection('phone_numbers')
  conn_loc = Mongo::Connection.new('localhost').db('tracks').collection('loc')
  tracks = []
  (10000000).times do |i|
    tracks << generate_track_log(i*30, conn_phone, conn_loc)
    j = 1
    puts "there are #{tracks.size} data in stack"
    if tracks.size == 1000
      conn.insert tracks
      tracks.clear
      puts "inserted the #{j}'s 10000 data"
      j+=1
    end
  end
end

gen_and_save_10Million_data
