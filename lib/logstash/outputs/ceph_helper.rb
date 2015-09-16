require "aws-sdk"
require "aws-sdk-core"

module LogStash::Outputs::CephHelper
  # TODO use logstash-plugins/logstash-mixin-aws

  US_EAST_1 = "us-east-1"

  REGIONS_ENDPOINT = [US_EAST_1, "us-west-1", "us-west-2", "eu-central-1",
                      "eu-west-1", "ap-southeast-1", "ap-southeast-2",
                      "ap-northeast-1", "sa-east-1", "us-gov-west-1", "cn-north-1"]

  def self.included(base)
    base.extend(self)
    base.generic_aws_config
  end

  def generic_aws_config
    config :endpoint, :validate => :string, :required => true

    config :access_key_id, :validate => :string, :required => true

    config :secret_access_key, :validate => :string, :required => true

    config :force_path_style, :validate => :boolean, :default => true

    config :region, :validate => REGIONS_ENDPOINT, :default => US_EAST_1

    config :session_token, :validate => :string

    config :aws_credentials_file, :validate => :string
  end

  def aws_options_hash
    opts = {}

    if @access_key_id.is_a?(NilClass) ^ @secret_access_key.is_a?(NilClass)
      @logger.warn("Likely config error: Only one of access_key_id or secret_access_key was provided but not both.")
    end

    opts[:credentials] = credentials if credentials

    opts[:endpoint] = @endpoint

    opts[:region] = @region

    opts[:force_path_style] = @force_path_style

    return opts
  end

  private
  def credentials
    @creds ||= begin
      if @access_key_id && @secret_access_key
        credentials_opts = {
            :access_key_id => @access_key_id,
            :secret_access_key => @secret_access_key
        }

        # credentials_opts[:session_token] = @session_token if @session_token
      elsif @aws_credentials_file
        credentials_opts = YAML.load_file(@aws_credentials_file)
      end

      if credentials_opts
        Aws::Credentials.new(credentials_opts[:access_key_id],
                             credentials_opts[:secret_access_key])
      end
    end
  end

end