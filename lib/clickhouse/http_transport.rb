# frozen_string_literal: true

require "http"
require "json"

module Clickhouse
  # HTTP transport layer for ClickHouse communication.
  # @api private
  class HttpTransport
    # Creates a new HTTP transport.
    #
    # @param config [Config] configuration instance
    def initialize(config)
      @config = config
      @http_client = HTTP.persistent("#{config.scheme}://#{config.host}:#{config.port}")
        .use(:auto_deflate)
        .use(:auto_inflate)
        .timeout(connect: config.connection_timeout)

      @default_headers = {
        "Accept-Encoding" => "gzip",
        "X-ClickHouse-User" => config.username,
        "X-ClickHouse-Key" => config.password,
        "X-ClickHouse-Format" => "Native"
      }
    end

    # Executes a SQL query via HTTP.
    #
    # @param sql [String] SQL query to execute
    # @param options [Hash] query options
    # @option options [Hash] :params query parameters
    # @return [TransportResult] result containing body and summary
    # @raise [QueryError] if the query fails
    def execute(sql, options = {})
      query_params = {database: @config.database}.merge(options[:params] || {})
      response = @http_client.post("/", params: query_params, body: sql, headers: @default_headers)

      summary = JSON.parse(response.headers["X-ClickHouse-Summary"], symbolize_names: true)

      raise QueryError, response.body.to_s unless response.status.success?

      TransportResult.new(body: response.body, summary: summary)
    end
  end
end
