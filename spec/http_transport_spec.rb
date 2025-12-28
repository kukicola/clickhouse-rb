# frozen_string_literal: true

RSpec.describe Clickhouse::HttpTransport do
  let(:config) { Clickhouse.config }
  let(:transport) { described_class.new(config) }

  describe "#execute" do
    it "returns TransportResult for valid query" do
      result = transport.execute("SELECT 1")

      expect(result).to be_a(Clickhouse::TransportResult)
      expect(result.summary).to be_a(Hash)
      expect(result.body).not_to be_nil
    end

    it "raises QueryError for invalid query" do
      expect {
        transport.execute("INVALID SQL SYNTAX")
      }.to raise_error(Clickhouse::QueryError, /Syntax error/)
    end
  end
end
