#!/usr/bin/env ruby


require_relative 'ceph_s3_accessor'

s3acc = CephS3Accessor.new

bktname = "my-test-bkt"
s3acc.create_bucket(bktname)
puts "have created bucket #{bktname}"

key = "d1/test_obj_name"
filename = "d1/obj5"
s3acc.upload_file(bktname, key, filename)
puts "have uploaded file #{filename} to  #{bktname}/#{key}"

bkts = s3acc.list_all_buckets

bkts.each do |bkt|
  puts "#{bkt.name} ::"
  objs = s3acc.list_one_bucket(bkt.name)
  objs.each do |obj|
    puts "\t#{obj.key}  #{obj.size}  #{obj.last_modified}  #{obj.etag}"
  end
end


download_file = "d1/download"
s3acc.download_file(bktname, key, download_file)
puts "have downloaded file #{bktname}/#{key} to file #{download_file}" 



s3acc.delete_object(bktname, key)
s3acc.delete_bucket(bktname)
puts "have deleted bucket #{bktname}"

#s3acc.list_one_bucket("hello-world")

bkts = s3acc.list_all_buckets
bkts.each do |bkt|
  puts "#{bkt.name} ::"
  objs = s3acc.list_one_bucket(bkt.name)
  objs.each do |obj|
    puts "\t#{obj.key}  #{obj.size}  #{obj.last_modified}  #{obj.etag}"
  end
end
