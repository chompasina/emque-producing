require "bunny"
require "thread"
require_relative "rabbitmq/channel"
require_relative "rabbitmq/connection"

module Emque
  module Producing
    module Publisher
      class RabbitMq < Emque::Producing::Publisher::Base
        Emque::Producing.configure do |c|
          c.ignored_exceptions =
            c.ignored_exceptions + [Bunny::Exception, Timeout::Error]
        end

        def initialize
          self.connection = Connection.new
        end

        def publish(topic, message_type, message, key = nil)
          result = false

          connection
            .channel_thread(message) { |channel, msg|
              channel.publish(
                topic,
                message,
                :mandatory => true,
                :persistent => true,
                :type => message_type,
                :app_id => Emque::Producing.configuration.app_name,
                :content_type => "application/json"
              )

              result = true if channel.status == :published
            }
            .join
          puts "done"

          result
        end

        private

        attr_accessor :connection
      end
    end
  end
end
