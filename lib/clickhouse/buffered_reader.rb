# frozen_string_literal: true

module Clickhouse
  # Buffered binary reader for streaming IO.
  # @api private
  class BufferedReader
    # Creates a new buffered reader.
    #
    # @param io [#readpartial] IO object supporting readpartial
    def initialize(io)
      @io = io
      @buffer = String.new(encoding: Encoding::BINARY)
      @eof = false
    end

    # Returns true if at end of stream.
    #
    # @return [Boolean]
    def eof?
      fill if @buffer.empty?
      @buffer.empty?
    end

    # Reads exactly n bytes from the stream.
    #
    # @param n [Integer] number of bytes to read
    # @return [String] binary string of n bytes
    def read(n)
      fill until @buffer.bytesize >= n
      @buffer.slice!(0, n)
    end

    # Reads a single byte from the stream.
    #
    # @return [Integer, nil] byte value (0-255) or nil if at EOF
    def read_byte
      fill if @buffer.empty?
      return if @buffer.empty?

      @buffer.slice!(0, 1).ord
    end

    # Drains remaining data from the stream.
    #
    # @return [void]
    def flush
      @buffer.clear
      nil while @io.readpartial
    end

    private

    def fill
      return if @eof

      chunk = @io.readpartial
      chunk ? @buffer << chunk : @eof = true
    end
  end
end
