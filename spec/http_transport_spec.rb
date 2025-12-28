# frozen_string_literal: true

RSpec.describe Clickhouse::HttpTransport do
  let(:config) { Clickhouse.config }
  let(:transport) { described_class.new(config) }

  describe "#execute" do
    it "returns success Result for valid query" do
      result = transport.execute("SELECT 1")

      expect(result).to be_a(Clickhouse::TransportResult)
      expect(result.success).to be true
      expect(result.error).to be_nil
      expect(result.summary).to be_a(Hash)
      expect(result.body).not_to be_nil
    end

    it "returns failure Result for invalid query" do
      result = transport.execute("INVALID SQL SYNTAX")

      expect(result.success).to be false
      expect(result.error).to include("Syntax error")
    end
  end
end
