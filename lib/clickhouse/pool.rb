# frozen_string_literal: true

require "connection_pool"

module Clickhouse
  # Thread-safe connection pool for ClickHouse.
  #
  # @example
  #   pool = Clickhouse::Pool.new
  #   response = pool.query("SELECT * FROM users")
  #
  # @example Concurrent usage
  #   pool = Clickhouse::Pool.new
  #   threads = 10.times.map do
  #     Thread.new { pool.query("SELECT 1") }
  #   end
  #   threads.each(&:join)
  class Pool
    # Creates a new connection pool.
    #
    # @param config [Config] configuration instance (defaults to global config)
    #   Pool size and timeout are read from config.pool_size and config.pool_timeout
    def initialize(config = Clickhouse.config)
      @pool = ConnectionPool.new(size: config.pool_size, timeout: config.pool_timeout) do
        Connection.new(config)
      end
    end

    # Executes a SQL query using a pooled connection.
    #
    # @param sql [String] SQL query to execute
    # @param options [Hash] query options
    # @option options [Hash] :params query parameters
    # @return [Response] query response with rows, columns, and metadata
    def query(sql, options = {})
      @pool.with { |conn| conn.query(sql, options) }
    end
  end
end
