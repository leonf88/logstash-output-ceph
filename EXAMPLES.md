CEPH_S3_ACCESSOR.rb

    #!/usr/bin/env ruby
    #
    
    require 'aws-sdk'
    require 'aws-sdk-core'
    require 'uuid'
    
    class CephS3Accessor
    
      def initialize(endpoint='http://192.168.1.20')
        @access_key = "619JH81O2ZN99H9A6UO1"
        @secret_key = "5MQpJs3E5xE03YI3Ld3K65M9g5LqvM23RUV0zGdx"
        @s3_region = "us-east-1"
    
        @begin_ts = Time.now.to_i
    
        @endpoint = endpoint
    
        @cred = Aws::Credentials.new(@access_key, @secret_key)
    
        ENV['AWS_ACCESS_KEY_ID'] = @access_key
        ENV['AWS_SECRET_ACCESS_KEY'] = @secret_key
        ENV['AWS_REGION'] = @s3_region
    
        Aws.config.update(
          endpoint: @endpoint,
          credentials: @cred,
          region: @s3_region,
          force_path_style: true
        )
    
        @s3 = Aws::S3::Resource.new
        @s3_client = Aws::S3::Client.new
    
        #TODO: check s3, s3_client validity.
    
      end
    
      public
      def create_bucket(bktname)
        @s3_client.create_bucket(bucket: bktname)
      end
    
      # force-delete an entire bucket, regardless if the bucket is empty.
      public
      def delete_bucket(bktname)
        Aws::S3::Bucket.new(bktname, client: @s3_client).clear!
        @s3_client.delete_bucket(bucket: bktname)
      end
    
      # Delete an object.
      public
      def delete_object(bktname, key)
        @s3_client.delete_object(bucket: bktname, key: key)
      end
    
      # return: a list of S3 bucket.
      public
      def list_all_buckets
        bkts = []
        resp = @s3_client.list_buckets
        resp.buckets.each do |bucket|
          bkts.push(bucket)
        end
        return bkts
      end
    
      # return: a list of S3 objects in the given bucket.
      public
      def list_one_bucket(bktname)
        objs = []
        bucket = @s3.bucket(bktname)
        bucket.objects.each do |obj|
          objs.push(obj)
        end
        return objs
      end
    
    
      public
      def upload_file(bktname, key, filename)
        obj = @s3.bucket(bktname).object(key)
        obj.upload_file(filename)
      end
    
    
      # Download an S3 object specified by "bktname/key", save to a file as "filename".
      public
      def download_file(bktname, key, filename)
        File.open(filename, 'wb') do |file|
          reap = @s3_client.get_object({bucket: bktname, key: key, }, target: file)
        end
      end
    
    
    end

CEPH_S3_ACCESSOR_TEST.rb

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
