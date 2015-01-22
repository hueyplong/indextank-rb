require 'yajl/json_gem'
require 'faraday'

module IndexTank
  class Document
    attr_reader :docid

    # :docid => a String or Symbol, that is no longer than 1024 bytes when UTF-8 encoded
    def initialize(document_url, docid)
      raise InvalidArgument , "docid too long. max is 1024 bytes and got #{String(docid).bytesize}" unless String(docid).bytesize <= 1024
      @docid = docid
      @conn = IndexTank.setup_connection(document_url) do |faraday|
        faraday.use DocumentResponseMiddleware
      end
    end

    def log_to_everything(message)
      log_message = "[IndexTank][#{self.docid}]: #{message}"
      Rails.logger.info log_message if defined?(Rails) and defined?(Rails.logger)
      Resque.logger.info log_message if defined?(Resque) and defined?(Resque.logger)
    end

    # the options argument may contain a :variables key
    # with a Hash from variable numbers to their float values
    # this variables can be used in the scoring functions
    # when sorting a search
    def add(fields, options = {})
      options.reverse_merge!(:docid => self.docid, :fields => fields, :tries => 0)

      log_to_everything("Adding to index with tries #{options[:tries]}")

      options[:tries] += 1
      resp = @conn.put do |req|
        req.url ""
        req.body = options.to_json
      end
      
      log_to_everything("Response status was: #{resp.status}")

      # if SOME_CONDITION and options[:tries] < 3
      if ![200,204].include?(resp.status) && options[:tries] < 5
        log_to_everything("We're gonna retry that in 10 seconds")
        sleep(10)
        self.add(fields, options)
      end

      log_to_everything("Returning from add index attempt with status: #{resp.status}")

      resp.status
    end

    def delete(options = {})
      options.merge!(:docid => self.docid)
      resp = @conn.delete do |req|
        req.url ""
        req.body = options.to_json
      end

      resp.status 
    end

    def update_variables(variables, options = {})
      options.merge!(:docid => self.docid, :variables => variables)
      resp = @conn.put do |req|
        req.url "variables"
        req.body = options.to_json
      end

      resp.status
    end

    # updates the categories of a given document
    # the categories argument should be a Hash from string
    # to string defining the value for each category defined
    # by this document.
    def update_categories(categories, options = {} )
      options.merge!(:docid => self.docid, :categories => categories)
      resp = @conn.put do |req|
        req.url "categories"
        req.body = options.to_json
      end

      resp.status 
    end
  end

  class DocumentResponseMiddleware < Faraday::Response::Middleware
    def on_complete(env)
      case env[:status]
      when 200
        nil # this is the expected code
      when 204
        nil # this is another expected code, for empty responses
      when 401
        raise InvalidApiKey
      when 409
        raise IndexInitializing
      when 404
        raise NonExistentIndex
      when 400
        raise InvalidArgument, env[:body]
      else
        raise UnexpectedHTTPException, env[:body]
      end
    end
  end
end
