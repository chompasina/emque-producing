module Emque
  module Producing
    module Publisher
      class RabbitMq < Emque::Producing::Publisher::Base
        class Connection
          MAX_CHANNEL_ATTEMPTS = 5

          def initialize
            puts "new connection"
            @channels = Queue.new
            @mutex = Mutex.new
            @session = new_session
            # @channels << Channel.new(session)
          end

          def channel_thread(message, &block)
            Thread.new do
              begin
                channel = fetch_channel
                block.call(channel, message)
              ensure
                enque_channel(channel) if channel
              end
            end
          end

          private

          attr_reader :channels, :mutex

          def clear_channels
            while channel = channels.pop do
              begin
                channel.close
              rescue
                # he's dead, Jim
              ensure
                break if channels.empty?
              end
            end unless channels.empty?
          end

          def enque_channel(channel)
            if channel.open?
              channel.reset!
              channels << channel
            end
          end

          def fetch_channel(attempt = 1)
            session
            channels.empty? ? Channel.new(session) : channels.pop
          rescue => e
            raise e unless attempt < MAX_CHANNEL_ATTEMPTS
            fetch_channel(attempt+1)
          end

          def session
            num = rand
            puts "1.#{num}"
            mutex.synchronize do
              puts "2.#{num}"
              unless @session.open?
                puts "3.#{num}"
                clear_channels
                @session = new_session
              end
            end
            puts "4.#{num}"
            @session
          end

          def new_session
            Bunny
              .new(
                Emque::Producing.configuration.rabbitmq_options[:url],
                :recover_from_connection_close => true
              )
              .tap { |conn|
                conn.start
                clear_channels
              }
          end
        end
      end
    end
  end
end
