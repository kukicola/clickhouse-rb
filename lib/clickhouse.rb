# frozen_string_literal: true

require_relative "clickhouse/version"
require_relative "clickhouse/null_instrumenter"
require_relative "clickhouse/config"
require_relative "clickhouse/transport_result"
require_relative "clickhouse/http_transport"
require_relative "clickhouse/connection"
require_relative "clickhouse/response"
require_relative "clickhouse/body_reader"
require_relative "clickhouse/native_format_parser"

# Ruby client for ClickHouse database with Native format support.
#
# @example Basic usage
#   Clickhouse.configure do |config|
#     config.host = "localhost"
#     config.port = 8123
#   end
#
#   conn = Clickhouse::Connection.new
#   response = conn.query("SELECT 1")
#
# @example Using connection pool
#   pool = Clickhouse::Pool.new
#   response = pool.query("SELECT * FROM users")
module Clickhouse
  # Base error class for all Clickhouse errors
  class Error < StandardError; end

  # Raised when a query fails (syntax error, unknown table, etc.)
  class QueryError < Error; end

  # Raised when encountering an unsupported ClickHouse data type
  class UnsupportedTypeError < Error; end

  # Returns the global configuration instance.
  #
  # @return [Config] the configuration instance
  def self.config
    @config ||= Config.new
  end

  # Yields the global configuration for modification.
  #
  # @yield [Config] the configuration instance
  # @return [void]
  def self.configure
    yield(config) if block_given?
  end
end
