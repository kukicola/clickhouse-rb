# frozen_string_literal: true

module Clickhouse
  # Immutable result from transport layer.
  # @api private
  #
  # @!attribute [r] success
  #   @return [Boolean] true if request succeeded
  # @!attribute [r] body
  #   @return [HTTP::Response::Body, nil] response body for successful requests
  # @!attribute [r] error
  #   @return [String, nil] error message for failed requests
  # @!attribute [r] summary
  #   @return [Hash] ClickHouse query summary
  TransportResult = Data.define(:success, :body, :error, :summary)
end
