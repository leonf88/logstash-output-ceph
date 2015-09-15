# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/mem_event_queue"

# This output will send events to the Ceph storage system.
# Besides, it provides the buffer store, like Facebook
# Scribe (https://github.com/facebookarchive/scribe).
# If the ceph is unavailable, it saves the events to
# a secondary store, then reads them and sends them to
# the primary store when it's available.
#
# Like Scribe, the design involves two buffers. Events
# are always buffred briefly in memory, then they are
# buffered to a secondary store if the primary store is down.
#
# For more information, see https://github.com/netskin/ceph-ruby
# and https://github.com/facebookarchive/scribe
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#    ceph {
#      temporary_path => "/tmp/logstash"        (optional)
#      max_items => 50                          (optional)
#      retry_interval => 5                      (optional)
#    }
#
class LogStash::Outputs::Ceph < LogStash::Outputs::Base

  config_name "ceph"

  # Set the directory where logstash will store the files before sending it to ceph
  # default to the current OS temporary directory in linux /tmp/hcd_logs/local_store/
  config :local_file_path, :validate => :string, :default => File.join(Dir.tmpdir, "hcd_logs/local_store/")

  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string

  # Set the size per file.
  config :max_file_size_mb, :validate => :number, :default => 4

  # Force flush data from memory to disk if 
  # 1. total in-memory data reached max_mem_mb And
  # 2. total data size on disk hasn't reached the max_disk_mb.
  # Set the total in-memory data size. 
  config :max_mem_mb, :valiadte => :number, :default => 40

  # Set the total on disk data size.
  config :max_disk_mb, :valiadte => :number, :default => 400

  # Set the number of seconds before flushing a new file.
  # Flush is always done in bucket.
  # Flush data to disk if a bucket's earliest message in the bucket is older than seconds_before_new_file.
  config :seconds_before_new_file, :validate => :number, :default => 5 * 60

  # Set the number of workers for flushing data out. 
  config :flush_worker_num, :validate => :number, :default => 5

  # Set the number of workers to upload data into ceph. 
  config :upload_worker_num, :validate => :number, :default => 5

  # Set the time in seconds for retrying the ceph store open
  config :retry_interval, :validate => :number, :default => 5

  # Set the file suffix. 
  config :file_suffix, :validate => :string, :default => ""

  # Set the root bucket to upload data.
  config :upload_root_bucket, :validate => :string, :default=> "data"

  # Partition the data before uploading.
  config :partition_fields, :validate => :array, :default=> []

  ######################################
  # TODO
  # TCP configuration for debug, should be replaced
  # by the ceph storage with AWS APIs
  require("net/http")
  config :host, :validate => :string, :default => "localhost"
  config :port, :validate => :number, :default => 80

  def upload_file(file)
    @logger.debug("Ceph: upload #{File.basename(file)}.")
    Net::HTTP.get(URI("http://#{@host}:#{@port}"))
  end
  #######################################

  # S3 bucket
  config :bucket, :validate => :string

  # AWS endpoint_region
  config :endpoint_region, :validate => [], :deprecated => 'Deprecated, use region instead.'

  # initialize the output
  public
  def register
    require "thread"

    # Queue of queue of events to flush to disk.
    @to_flush_queue = Queue.new
    # Queue of files to upload.
    @file_queue = Queue.new
    # Max data size in memory.
    @max_mem_size = @max_mem_mb * 1024 * 1024
    # Max data size on disk.
    @max_disk_size = @max_disk_mb * 1024 * 1024
    # Current memory data size.
    @total_mem_size = 0
    # Current disk data size.
    @total_disk_size = 0
    
    @max_file_size = @max_file_size_mb * 1024 * 1024

    # TODO
    @file_counter = 0

    # A map from partitions to event queue
    @part_to_events = {}
    @part_to_events_lock = Mutex.new
    @part_to_events_condition = ConditionVariable.new

    @local_file_path = File.expand_path(@local_file_path)
    add_existing_disk_files()

    # Move event queues to to_flush_queue 
    start_queue_mover()
    start_upload_workers()
    start_flush_workers()
    print "start over"
    p @total_mem_size
  end

  public
  def teardown(force = false)
    if force
      @logger.debug("Ceph: shutdown the upload worker forcefully.")
      @upload_worker.stop
    else
      @logger.debug("Ceph: gracefully shutdown the upload worker.")
      @file_queue << LogStash::ShutdownEvent
      @upload_worker.join
    end
  end

  public
  def add_existing_disk_files()
    if !Dir.exists?(@local_file_path)
      @logger.info("Create directory", :directory => @local_file_path)
      FileUtils.mkdir_p(@local_file_path)
    else
      # Scan the folder and load all existing files.
      cwd = Dir.pwd
      Dir.chdir(@local_file_path) # multi-threaded program may throw an error
      Dir.glob(File.join(@local_file_path, "**", "*")).each do |file_name|
        @file_queue << file_name if !File.directory? file_name
        @total_disk_size += File.size(file_name)
      end
      Dir.chdir(cwd)
    end
  end

  public
  def start_queue_mover()
    @move_queue_thread = Thread.new {
      loop do
        @part_to_events_lock.synchronize {
          p @part_to_events.size
          @part_to_events.each do |part, event_queue|
            p event_queue.seconds_since_first_event, @seconds_before_new_file
            if event_queue.seconds_since_first_event > @seconds_before_new_file || event_queue.total_size >= @max_file_size
              p "flush a queue"
              @to_flush_queue << event_queue
              @part_to_events.delete(part)
            end
          end
        }
        sleep(@seconds_before_new_file * 0.7)
      end
    }
  end

  public
  def start_flush_workers()
    @flush_workers =[]
    if @flush_worker_num == 0
      @flush_worker_num = 1
    end

    @flush_worker_num.times do
      @flush_workers << Thread.new {flush_worker()}
    end
  end

  public
  def start_upload_workers()
    @upload_workers = []
    if @upload_worker_num == 0
      @upload_worker_num = 1
    end

    @upload_worker_num.times do
      @upload_workers << Thread.new {upload_worker()}
    end
  end

  public
  def receive(event)
    return unless output?(event)

    # @buffer_items << format_message(event)
    
    # use json format to calculate partitions and event size.
    json_event = event.to_json

    if @partition_fields
      partitions = get_partitions(json_event)
    else
      partitions = [""]
    end
    
    # Block when the total mem size reached up limit.
    wait_for_mem

    @part_to_events_lock.synchronize {
      if ! @part_to_events.has_key?(partitions)
        @part_to_events[partitions] = MemEventQueue.new(partitions)
      end
      event_queue = @part_to_events.fetch(partitions)
      # p partitions.id, event_queue.seconds_since_first_event, @seconds_before_new_file
       p event_queue.seconds_since_first_event, @seconds_before_new_file
      if event_queue.seconds_since_first_event > @seconds_before_new_file || event_queue.total_size >= @max_file_size
        @to_flush_queue << event_queue
        event_queue = MemEventQueue.new(partitions)
        @part_to_events[partitions] = event_queue
      end

      p "push event"
      event_queue.push(event, json_event.size)
      @total_mem_size += json_event.size
    }

  end # def event

  public
  def wait_for_mem()
    @part_to_events_lock.synchronize {
      while @total_mem_size >= @max_mem_size
        if @to_flush_queue.empty?
          # wait for the flush
          @part_to_events_condition.wait(@part_to_events_lock)
        else
          # WARN: rarely can execute here
          # flush the latest partition, the time may not be accurate if the queue is too large
          latest_partition = @part_to_events.max_by { |part, event_queue| event_queue.seconds_since_first_event }
          @to_flush_queue << latest_partition[1]
          @part_to_events.delete(latest_partition[0])
        end
      end
    }
  end

  public
  def get_partitions(json_event)
    ret = []
    @partition_fields.each do |part|
      ret << part + "=" + json_event[part]
    end
  end

  # flush the memory events on local disk
  private
  def flush_worker()
    loop do
      event_queue = @to_flush_queue.deq
      p "get a queue"
      partition_dir = @local_file_path
      event_queue.partitions.each do |part|
        partition_dir = File.join(partition_dir, part)
      end

      if !Dir.exists?(partition_dir)
        @logger.info("Create directory", :directory => partition_dir)
        FileUtils.mkdir_p(partition_dir)
      end
      p partition_dir

      #flush data to disk.
      @file_queue << write_to_tempfile(event_queue.event_queue(), partition_dir)

      @part_to_events_lock.synchronize {
        @total_mem_size -= event_queue.total_size
        @part_to_events_condition.signal
      }
    end
  end

  # upload the local temporary files to ceph
  private
  def upload_worker()
    loop do
      file = @file_queue.deq

      case file
        when LogStash::ShutdownEvent
          @logger.debug("Ceph: upload worker is shutting down gracefuly")
          break
        else
          @logger.debug("Ceph: upload working is uploading a new file", :filename => File.basename(file))
          move_file_to_bucket(file)
      end
    end
  end

  # flush the local files to ceph storage
  # if success, delete the local file, otherwise, retry
  private
  def move_file_to_bucket(file)
    begin
      if !File.zero?(file)
        upload_file(file)
      end
    rescue => e
      @logger.warn("Failed to upload the chunk file to ceph. Retry after #{@retry_interval} seconds.", :exception => e)
      sleep(@retry_interval)
      retry
    end

    begin
      File.delete(file)
    rescue Errno::ENOENT
      # Something else deleted the file, logging but not raising the issue
      @logger.warn("Ceph: Cannot delete the file since it doesn't exist on disk", :filename => File.basename(file))
    rescue Errno::EACCES
      @logger.error("Ceph: Logstash doesnt have the permission to delete the file in the temporary directory.", :filename => File.basename(file), :local_file_path => @local_file_path)
    end
  end

  private
  def write_to_tempfile(events, dir)
    filename = create_temporary_file(dir)
    fd = File.open(filename, "w")
    begin
      @logger.debug("Ceph: put events into tempfile ", :file => File.basename(fd.path))
      while ! events.empty?
        fd.syswrite(format_message(events.pop(non_block = true)))
      end
    rescue Errno::ENOSPC
      @logger.error("Ceph: No space left in temporary directory", :local_file_path => @local_file_path)
      teardown(true)
    ensure
      fd.close
    end
    return fd.path
  end

  # format the output message
  private
  def format_message(event)
    if @message_format
      event.sprintf(@message_format)
    else
      event.to_json
    end
  end

  private
  def create_temporary_file(dir)
    current_time = Time.now
    filename = File.join(dir, "ceph.store.#{current_time.strftime("%Y-%m-%dT%H.%M")}.part#{@file_counter}")
    @logger.info("Opening file", :path => filename)
    @file_counter += 1

    return filename
  end
end
