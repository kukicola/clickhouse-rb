# frozen_string_literal: true

RSpec.describe Clickhouse::BufferedReader do
  let(:mocked_chunked_io) do
    # Mock IO that simulates chunked reading like HTTP::Response::Body
    Class.new do
      def initialize(data, chunk_size: 4)
        @data = data.b
        @chunk_size = chunk_size
        @position = 0
      end

      def readpartial
        return nil if @position >= @data.bytesize

        chunk = @data[@position, @chunk_size]
        @position += @chunk_size
        chunk
      end
    end
  end

  describe "#eof?" do
    it "returns false when data is available" do
      io = mocked_chunked_io.new("hello")
      reader = described_class.new(io)

      expect(reader.eof?).to be false
    end

    it "returns true when no data is available" do
      io = mocked_chunked_io.new("")
      reader = described_class.new(io)

      expect(reader.eof?).to be true
    end

    it "returns true after all data is consumed" do
      io = mocked_chunked_io.new("hi")
      reader = described_class.new(io)

      reader.read(2)

      expect(reader.eof?).to be true
    end
  end

  describe "#read" do
    it "reads exact number of bytes" do
      io = mocked_chunked_io.new("hello world")
      reader = described_class.new(io)

      expect(reader.read(5)).to eq("hello")
      expect(reader.read(1)).to eq(" ")
      expect(reader.read(5)).to eq("world")
    end

    it "reads across chunk boundaries" do
      io = mocked_chunked_io.new("abcdefghij", chunk_size: 3)
      reader = described_class.new(io)

      expect(reader.read(7)).to eq("abcdefg")
      expect(reader.read(3)).to eq("hij")
    end

    it "returns binary encoded string" do
      io = mocked_chunked_io.new("\x00\x01\x02\x03")
      reader = described_class.new(io)

      result = reader.read(4)

      expect(result.encoding).to eq(Encoding::BINARY)
      expect(result.bytes).to eq([0, 1, 2, 3])
    end
  end

  describe "#read_byte" do
    it "reads single byte as integer" do
      io = mocked_chunked_io.new("\x00\x7F\xFF")
      reader = described_class.new(io)

      expect(reader.read_byte).to eq(0)
      expect(reader.read_byte).to eq(127)
      expect(reader.read_byte).to eq(255)
    end

    it "returns nil at end of stream" do
      io = mocked_chunked_io.new("a")
      reader = described_class.new(io)

      reader.read_byte

      expect(reader.read_byte).to be_nil
    end
  end

  describe "mixed reads" do
    it "handles interleaved read and read_byte calls" do
      io = mocked_chunked_io.new("\x05hello")
      reader = described_class.new(io)

      length = reader.read_byte
      expect(length).to eq(5)

      data = reader.read(length)
      expect(data).to eq("hello")
    end
  end

  describe "#flush" do
    it "drains remaining data from stream" do
      io = mocked_chunked_io.new("hello world")
      reader = described_class.new(io)

      reader.read(5)
      reader.flush

      expect(reader.eof?).to be true
    end

    it "clears the buffer" do
      io = mocked_chunked_io.new("hello")
      reader = described_class.new(io)

      reader.read(2)
      reader.flush

      expect(reader.eof?).to be true
    end
  end
end
