#!/usr/bin/ruby
require "rubygems"
require "sinatra"
require "haml"
require "mongo"

# Haml::Template.options[:format] = :html5
helpers do
  def get_conn(coll="track_log")
    Mongo::Connection.new('localhost').db('test').collection(coll)
  end
  
  def insert(data)
    get_conn.insert data
  end
  
  def search(str)
    get_conn.find(str)
  end
  
  def coll_count(coll='track_log')
    get_conn(coll).count
  end
  
  def get_a_random_row(coll='track_log')
    get_conn(coll).find_one
  end
  
  def add_index(field)
    get_conn.create_index field
  end
end

get '/' do
 haml :index
end

get '/hi' do
  haml :index
end

post '/query' do

  query = {params[:field_name] => params[:field_value]}
  @results = search(query)
  haml :result
end



__END__
@@index
!!! 5
%title Query With MongoDB
#content
	%form{:method => 'post', :action =>'/query'}
		#key_value_query
		%label{:for => 'field_name'} 字段名：
		%input{:id => 'field_name', :name => 'field_name', :type=>'text'}
		%label{:for => 'field_value'} 值：
		%input{:type => 'text', :id => 'field_value', :name => 'field_value'}
		%button{:type => 'search', :id => 'query'}搜索
		
@@result
!!! 5
%title 查询结果
#result
  %table{:board  => 4, :padding => 5}
    %tr
      %th device_no
      %th sim
      %th 类型
      %th GPS时间
      %th Valid
      %th 经度
      %th 纬度
      %th 海拔
      %th 速度
      %th 方向
      %th 里程
      %th 参数
      %th 接收时间      
    -@results.each do |row|
      %tr
        %td= row["device_no"]
        %td= row["sim"]
        %td= row["type"]
        %td= row["GPS_time"]
        %td= row["valid"]
        %td= row["long"]
        %td= row["lat"]
        %td= row["altitude"]
        %td= row["speed"]
        %td= row["navigation_course"]
        %td= row["KM"]
        %td= row["parameter"]
        %td= row["receive_time"]

  