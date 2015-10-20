module ActiveJob
  module QueueAdapters
    class NewTestAdapter

      module Compatibility
        def perform_enqueued_jobs
          @perform_when_enqueued
        end

        def perform_enqueued_jobs=(val)
          @perform_when_enqueued= val
        end

        def perform_enqueued_at_jobs
          (@perform_when_enqueued && @check_when_enqueued_at)
        end

        def perform_enqueued_at_jobs=(val)
        end

        def enqueued_jobs=(jobs)
        end

        def performed_jobs=(jobs)
        end

        private
        def job_to_hash(job, extras = {})
          { job: job.class, args: job.serialize.fetch('arguments'), queue: job.queue_name }.merge!(extras)
        end
      end

      attr_accessor :filter

      def initialize
        @queues         = HashWithIndifferentAccess.new { |h, k| h[k] = [] }
        @performed_jobs = []
        @failed_jobs    = []
        @waiting_jobs   = SortedSet.new
        @filter         = nil

        @perform_when_enqueued  = false
        @check_when_enqueued_at = false
      end

      def enqueue(job)
        return if filtered?(job)

        @queues[job.queue_name] << job.serialize

        perform_jobs! if @perform_when_enqueued
      end

      def enqueue_at(job, timestamp)
        return if filtered?(job)

        @waiting_jobs.add(TimeSortableJob.new(job.serialize, timestamp))

        check_for_waiting_jobs! if @check_when_enqueued_at
      end

      # Pulls jobs off queues and executes them.
      #
      # If both count and queue_name are specified, this
      # method will remove up to *count* jobs from the 
      # queue specified by *queue_name* and execute them.
      #
      # If queue_name is specified but count is not, this
      # will execute all jobs from that queue.
      #
      # If neither queue_name nor count are specified, this
      # will execute all jobs from all queues (the order
      # being undefined)
      #
      # Finally, if count is specified but queue_name is not,
      # this will execute up to *count* jobs from any queue,
      # the queue priority being arbitrary.  For instance, if
      # queue 'a' had 2 jobs and queue 'b' had 2 jobs, 
      # calling `perform_jobs! count: 4` would execute both
      # the jobs from 'a' and 'b'.
      #
      def perform_jobs!(count: nil, queue_name: nil)
        catch(:queue_empty) do
          loop_n(count) do
            perform_one(queue_name)
          end
        end
      end

      def check_for_waiting_jobs!
        next_job = @waiting_jobs.first
        if next_job.timestamp <= DateTime.now
          @waiting_jobs.delete(next_job)
          self.enqueue(next_job)
        end
      end

      def enqueued(queue_name=nil)
        queue_name ? @queues.values.flatten : @queues[queue_name]
      end

      def queue_length(queue_name)
        @queues[queue_name].length
      end

      private
      def filtered?(job)
        filter && !Array(filter).include?(job.class)
      end

      def loop_n(n=nil, &block)
        if n
          n.times { yield }
        else
          loop { yield }
        end
      end

      def perform_one(queue_name=nil)
        if(queue_name.nil?)
          catch(:queue_empty) do
            perform_one @queues.keys.shuffle.first
          end
          throw(:queue_empty if @queues.empty?
        else
          serialized_job = @queues[queue_name].shift

          if serialized_job.nil?
            @queues.delete(queue_name)
            throw(:queue_empty)
          else
            perform_job(serialized_job)
          end
        end
      end

      def perform_job(serialized_job)
        begin
          ::ActiveJob::Base.execute serialized_job
        rescue => err
        ensure
          if err.nil?
            @performed_jobs << serialized_job
          else
            @failed_jobs << [serialized_job, err]
          end
        end
      end

      class TimeSortableJob
        attr_reader :serialized_job, :timestamp

        def initialize(serialized_job, timestamp)
          @serialized_job = serialized_job
          @timestamp = timestamp
        end

        def <=>(other)
          self.timestamp <=> other.timestamp
        end
      end
    end
  end
end
