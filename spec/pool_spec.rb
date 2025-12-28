# frozen_string_literal: true

RSpec.describe Clickhouse::Pool do
  let(:pool) { described_class.new }

  describe "#query" do
    it "executes query and returns response" do
      response = pool.query("SELECT 1 AS value")

      expect(response.rows).to eq([[1]])
    end

    it "handles multiple concurrent queries" do
      threads = 5.times.map do |i|
        Thread.new { pool.query("SELECT #{i} AS value") }
      end

      responses = threads.map(&:value)

      expect(responses.map { |r| r.rows.first.first }).to match_array([0, 1, 2, 3, 4])
    end

    it "raises QueryError for invalid query" do
      expect {
        pool.query("INVALID SQL")
      }.to raise_error(Clickhouse::QueryError)
    end
  end
end
