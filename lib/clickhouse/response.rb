# frozen_string_literal: true

module Clickhouse
  # Immutable response object containing query results.
  #
  # @example
  #   response = conn.query("SELECT id, name FROM users")
  #   response.columns   # => ["id", "name"]
  #   response.rows      # => [[1, "Alice"], [2, "Bob"]]
  #   response.to_a      # => [{"id" => 1, "name" => "Alice"}, ...]
  Response = Data.define(:columns, :types, :rows, :summary) do
    # @param columns [Array<String>] column names
    # @param types [Array<String>] column types
    # @param rows [Array<Array>] row data
    # @param summary [Hash, nil] ClickHouse query summary
    def initialize(columns: [], types: [], rows: [], summary: nil)
      super
    end

    # Converts rows to an array of hashes.
    # @return [Array<Hash>] rows as hashes with column names as keys
    def to_a = rows.map { |row| columns.zip(row).to_h }
  end
end
