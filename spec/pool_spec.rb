# frozen_string_literal: true

RSpec.describe Clickhouse::Pool do
  let(:pool) { described_class.new }

  describe "#query" do
    it "executes query and returns response" do
      response = pool.query("SELECT 1 AS value")

      expect(response).to be_success
      expect(response.rows).to eq([[1]])
    end

    it "handles multiple concurrent queries" do
      threads = 5.times.map do |i|
        Thread.new { pool.query("SELECT #{i} AS value") }
      end

      responses = threads.map(&:value)

      expect(responses).to all(be_success)
      expect(responses.map { |r| r.rows.first.first }).to match_array([0, 1, 2, 3, 4])
    end

    it "returns error response for invalid query" do
      response = pool.query("INVALID SQL")

      expect(response).to be_failure
    end
  end
end
