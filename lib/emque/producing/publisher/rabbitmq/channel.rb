module Emque
  module Producing
    module Publisher
      class RabbitMq < Emque::Producing::Publisher::Base
        class Channel
          attr_reader :status

          def initialize(session)
            puts "new channel"
            @session = session
            # channel.open
            reset!
          end

          def publish(topic, message, *args)
            set_status :publishing
            channel.open unless channel.open?
            exchange(topic, message).publish(message, *args)
            set_status :published
            wait_for_confirms
          end

          def reset!
            @status = :idle
          end

          private

          attr_reader :session

          def channel
            @channel ||= session.create_channel
          end

          def method_missing(meth, *attrs, &block)
            channel.send(meth, *attrs, &block)
          end

          def exchange(topic, message)
            channel
              .fanout(topic, :durable => true, :auto_delete => false)
              .tap { |xchg|
                # Assumes all messages are mandatory in order to let callers
                # know if the message was not sent. Uses publisher confirms
                # to wait.
                channel.confirm_select
              }
              .on_return { |return_info, properties, content|
                Emque::Producing
                  .logger
                  .warn(
                    "App [#{properties[:app_id]}] message was returned " +
                    "from exchange [#{return_info[:exchange]}]"
                  )
                set_status :failed
              }
          end

          def set_status(sym)
            @status = sym unless status == :failed
          end

          def wait_for_confirms
            unless channel.wait_for_confirms
              Emque::Producing
                .logger
                .warn("RabbitMQ Publisher: message was nacked")

              channel.nacked_set.each do |n|
                Emque::Producing.logger.warn("message id: #{n}")
              end
            end
          end
        end
      end
    end
  end
end

