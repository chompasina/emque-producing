require "securerandom"
require "socket"

module Emque
  module Producing
    module Message
      InvalidMessageError = Class.new(StandardError)
      MessagesNotSentError = Class.new(StandardError)

      module ClassMethods
        def topic(name)
          @topic = name
        end

        def read_topic
          @topic
        end

        def message_type(name)
          @message_type = name
        end

        def read_message_type
          @message_type
        end

        def mandatory(name)
          @mandatory = name
        end

        def read_mandatory
          @mandatory || true
        end

        def requires_confirmation(name)
          @requires_confirmation = name
        end

        def read_requires_confirmation
          @requires_confirmation || true
        end

        def private_attribute(name, coercion=nil, opts={})
          @private_attrs ||= []
          @private_attrs << name
          attribute(name, coercion, opts)
        end

        def private_attrs
          Array(@private_attrs)
        end
      end

      def self.included(base)
        base.extend(ClassMethods)
        base.send(:include, Virtus.model)
        base.send(:attribute, :partition_key, String, :default => nil, :required => false)
      end

      def add_metadata
        {
          :metadata =>
          {
            :host => host_name,
            :app => app_name,
            :topic => topic,
            :created_at => formatted_time,
            :uuid => uuid,
            :type => message_type,
            :partition_key => partition_key
          }
        }.merge(public_attributes)
      end

      def topic
        self.class.read_topic
      end

      def message_type
        self.class.read_message_type
      end

      def mandatory
        self.class.read_mandatory
      end

      def requires_confirmation
        self.class.read_requires_confirmation
      end

      def valid?
        invalid_attributes.empty? && topic && message_type
      end

      def invalid_attributes
        invalid_attrs = self.class.attribute_set.inject([]) do |attrs, attr|
          attrs << attr.name if attr.required? && self.attributes.fetch(attr.name).nil?
          attrs
        end
        Array(invalid_attrs) - self.class.private_attrs
      end

      def to_json
        data = self.add_metadata
        Oj.dump(data, :mode => :compat)
      end

      def publish(publisher=Emque::Producing.publisher)
        log "publishing...", true
        if valid?
          log "valid...", true
          if Emque::Producing.configuration.publish_messages
            sent = publisher.publish(
              topic, message_type, to_json, mandatory, requires_confirmation, partition_key
            )
            log "sent #{sent}"
            raise MessagesNotSentError.new unless sent
          end
        else
          log "failed...", true
          raise InvalidMessageError.new(invalid_message)
        end
      end

      private

      def invalid_message
        if !topic
          "A topic is required"
        elsif !message_type
          "A message type is required"
        else
          "Required attributes #{invalid_attributes} are missing."
        end
      end

      def host_name
        Socket.gethostbyname(Socket.gethostname).first
      end

      def formatted_time
        DateTime.now.new_offset(0).to_time.utc.iso8601
      end

      def uuid
        SecureRandom.uuid
      end

      def app_name
        Emque::Producing.configuration.app_name || raise("Messages must have an app name configured.")
      end

      def log(message, include_message = false)
        if Emque::Producing.configuration.log_publish_message
          message = "#{message} #{to_json}" if include_message
          Emque::Producing.logger.info("MESSAGE LOG: #{message}")
        end
      end

      def public_attributes
        public = self.class.attribute_set.select do |attr|
          attr && !self.class.private_attrs.include?(attr.name)
        end.map(&:name)
        slice_attributes(*public)
      end

      def slice_attributes(*keys)
        keys.map!(&:to_sym)
        attributes.select { |key, value| keys.include?(key) }
      end
    end
  end
end
