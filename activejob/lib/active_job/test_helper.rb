require 'active_support/core_ext/class/subclasses'
require 'active_support/core_ext/hash/keys'

module ActiveJob
  module TestHelper
    extend ActiveSupport::Concern

    module Compatibility
      def perform_enqueued_jobs(only: nil)
        queue_adapter.perform_immediately_within(only: only) do 
          yield
        end
      end

      def assert_performed_jobs(number, only: nil)
        if block_given?
          performed_jobs = queue_adapter.perform_immediately_within(only: only) do 
            queue_adapter.performed_within do
              yield
            end
          end

          if only
            performed_jobs.select! { |job| Array(only).include?(job['job_class'].constantize) }
          end

          assert_equal number, performed_jobs.length, "#{number} jobs expected, but #{performed_jobs.length} were performed" 
        else
          actual_count = performed_jobs_size(only: only)
          assert_equal number, actual_count, "#{number} jobs expected, but #{actual_count} were performed"
        end
      end

      def assert_no_performed_jobs(only: nil, &block)
        assert_performed_jobs(0, only: only, &block)
      end

      def assert_enqueued_jobs(number, only: nil)
        if block_given?
          enqueued_jobs = queue_adapter.enqueued_within { yield }

          if only
            enqueued_jobs.select! { |job| Array(only).include?(job['job_class'].constantize) }
          end

          assert_equal number, enqueued_jobs.length, "#{number} jobs expected, but #{enqueued_jobs.length} were enqueued"
        else
          actual_count = enqueued_jobs_size(only: only)
          assert_equal number, actual_count, "#{number} jobs expected, but #{actual_count} were enqueued"
        end
      end

      def assert_no_enqueued_jobs(only: nil, &block)
        assert_enqueued_jobs(0, only: only, &block)
      end

      def enqueued_jobs_size(only: nil)
        if only
          queue_adapter.find_enqueued_jobs do |serialized_job|
            Array(only).include?(serialized_job['job_class'].constantize)
          end.length
        else
          queue_adapter.find_enqueued_jobs.length
        end
      end

      def performed_jobs_size(only: nil)
        if only
          queue_adapter.find_performed_jobs do |serialized_job|
            Array(only).include?(serialized_job['job_class'].constantize)
          end.length
        else
          queue_adapter.find_performed_jobs.length
        end
      end


      def assert_enqueued_with(args={})
        args.assert_valid_keys(:job, :args, :at, :queue)

        enqueued_jobs = queue_adapter.enqueued_within { yield }
        deserialized_jobs = queue_adapter.deserialize_all(enqueued_jobs)
        deserialized_jobs.map { |dj| dj.send(:deserialize_arguments_if_needed) }

        matching_job = deserialized_jobs.find(nil) do |job|
          next false if args[:job]   && args[:job]        != job.class
          next false if args[:queue] && args[:queue].to_s != job.queue_name
          next false if args[:args]  && args[:args]       != job.arguments
          next false if args[:at]    && args[:at]         != job.scheduled_at
          true
        end

        assert matching_job, "No enqueued job found with #{args}"
        matching_job
      end

      def assert_performed_with(args={})
        args.assert_valid_keys(:job, :args, :queue)

        performed_jobs = queue_adapter.perform_immediately_within do
          queue_adapter.performed_within { yield }
        end

        deserialized_jobs = queue_adapter.deserialize_all(performed_jobs)
        deserialized_jobs.map { |dj| dj.send(:deserialize_arguments_if_needed) }

        matching_job = deserialized_jobs.find(nil) do |job|
          next false if args[:job]   && args[:job]        != job.class
          next false if args[:queue] && args[:queue].to_s != job.queue_name
          next false if args[:args]  && args[:args]       != job.arguments
          true
        end

        assert matching_job, "No performed job found with #{args}"
        matching_job
      end


      def serialize_args_for_assertion(args) # :nodoc:
        args.dup.tap do |serialized_args|
          serialized_args[:args] = ActiveJob::Arguments.serialize(serialized_args[:args]) if serialized_args[:args]
          serialized_args[:at]   = serialized_args[:at].to_f if serialized_args[:at]
        end
      end
    end

    included do
      include Compatibility

      def before_setup # :nodoc:
        test_adapter = ActiveJob::QueueAdapters::TestAdapter.new

        @old_queue_adapters = (ActiveJob::Base.subclasses << ActiveJob::Base).select do |klass|
          klass.singleton_class.public_instance_methods(false).include?(:_queue_adapter)
        end.map do |klass|
          [klass, klass.queue_adapter].tap do
            klass.queue_adapter = test_adapter
          end
        end

        super
      end

      def after_teardown # :nodoc:
        super
        @old_queue_adapters.each do |(klass, adapter)|
          klass.queue_adapter = adapter
        end
      end

      def perform_jobs!(n = nil)
        queue_adapter.perform(n)
      end

      def queue_adapter
        ActiveJob::Base.queue_adapter
      end

      delegate :enqueued_jobs, :enqueued_jobs=,
               :performed_jobs, :performed_jobs=,
               to: :queue_adapter
    end
  end
end
