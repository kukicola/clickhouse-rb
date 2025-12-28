# frozen_string_literal: true

module Clickhouse
  # Immutable response object containing query results.
  #
  # @example Successful response
  #   response = conn.query("SELECT id, name FROM users")
  #   response.success?  # => true
  #   response.columns   # => ["id", "name"]
  #   response.rows      # => [[1, "Alice"], [2, "Bob"]]
  #   response.to_a      # => [{"id" => 1, "name" => "Alice"}, ...]
  #
  # @example Failed response
  #   response = conn.query("INVALID SQL")
  #   response.failure?  # => true
  #   response.error     # => "Syntax error..."
  Response = Data.define(:columns, :types, :rows, :error, :summary) do
    # @param columns [Array<String>] column names
    # @param types [Array<String>] column types
    # @param rows [Array<Array>] row data
    # @param error [String, nil] error message if query failed
    # @param summary [Hash, nil] ClickHouse query summary
    def initialize(columns: [], types: [], rows: [], error: nil, summary: nil)
      super
    end

    # Returns true if the query succeeded.
    # @return [Boolean]
    def success? = error.nil?

    # Returns true if the query failed.
    # @return [Boolean]
    def failure? = !success?

    # Converts rows to an array of hashes.
    # @return [Array<Hash>] rows as hashes with column names as keys
    def to_a = rows.map { |row| columns.zip(row).to_h }
  end
end
