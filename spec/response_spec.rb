# frozen_string_literal: true

RSpec.describe Clickhouse::Response do
  describe "#initialize" do
    it "has default empty values" do
      response = described_class.new

      expect(response.columns).to eq([])
      expect(response.types).to eq([])
      expect(response.rows).to eq([])
      expect(response.summary).to be_nil
    end

    it "accepts custom values" do
      response = described_class.new(
        columns: ["id", "name"],
        types: ["UInt32", "String"],
        rows: [[1, "Alice"], [2, "Bob"]],
        summary: {"read_rows" => "2"}
      )

      expect(response.columns).to eq(["id", "name"])
      expect(response.types).to eq(["UInt32", "String"])
      expect(response.rows).to eq([[1, "Alice"], [2, "Bob"]])
      expect(response.summary).to eq({"read_rows" => "2"})
    end
  end

  describe "#to_a" do
    it "returns rows as array of hashes" do
      response = described_class.new(
        columns: ["id", "name"],
        rows: [[1, "Alice"], [2, "Bob"]]
      )

      expect(response.to_a).to eq([
        {"id" => 1, "name" => "Alice"},
        {"id" => 2, "name" => "Bob"}
      ])
    end

    it "returns empty array when no rows" do
      response = described_class.new

      expect(response.to_a).to eq([])
    end
  end
end
