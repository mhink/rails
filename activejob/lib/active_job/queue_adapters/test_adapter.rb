module ActiveJob
  module QueueAdapters
    class TestAdapter
      module Compatibility
        def enqueued_jobs
          find_enqueued_jobs
        end

        def performed_jobs
          find_performed_jobs
        end

        def perform_enqueued_jobs=(val)
          @perform_immediately = val
        end
        alias perform_enqueued_at_jobs= perform_enqueued_jobs=
      end

      module AdapterMethods
        def filtered?(job)
          return false if filter.nil?

          if filter.respond_to?(:call)
            filter.call(job)
          elsif filter.is_a? Class
            job.class != filter
          elsif filter.respond_to?(:include?)
            !filter.include? job.class
          end
        end

        def enqueue(job)
          return if filtered? job

          serialized_job = job.serialize

          @queues[job.queue_name] << serialized_job
          @tracked_enqueued_jobs << serialized_job unless (@tracked_enqueued_jobs == nil)

          if @perform_immediately
            perform_job(serialized_job)
          end
        end

        def enqueue_at(job, timestamp)
          return if filtered? job

          if @perform_immediately
            enqueue(job)
          else
            time_sortable_job = TimeSortableJob.new(job.serialize, timestamp)
            @waiting_jobs.add time_sortable_job
            @tracked_enqueued_jobs << time_sortable_job unless (@tracked_enqueued_jobs == nil)
          end
        end

        # @waiting_jobs is a SortedSet, whose elements are TimeSortableJobs.
        # So, the first element of @waiting_jobs should be the job with the
        # earliest #scheduled_at time. This method inspects that time and
        # enqueues the job for execution if its scheduled execution time has
        # passed.
        def check_for_waiting_jobs!
          next_job = @waiting_jobs.first
          if Time.at(next_job.timestamp) <= DateTime.now
            @waiting_jobs.delete(next_job)

            self.enqueue(deserialize_all([next_job]).first)
          end
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
        def perform_jobs!(count: nil, queue_name: ANY_QUEUE)
          catch(:queue_empty) do
            loop_n(count) do
              perform_one(queue_name)
            end
          end
        end

        protected
        def perform_one(queue_name=ANY_QUEUE)
          if(queue_name == ANY_QUEUE)
            throw(:queue_empty) if @queues.empty?
            catch(:queue_empty) do
              perform_one @queues.keys.shuffle.first
            end
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

        def loop_n(n=nil, &block)
          if n
            n.times { yield }
          else
            loop { yield }
          end
        end

        def perform_job(serialized_job)
          begin
            ::ActiveJob::Base.execute serialized_job
          rescue => err
          ensure
            if err.nil?
              @performed_jobs         << serialized_job
              @tracked_performed_jobs << serialized_job unless @tracked_performed_jobs.nil?
            else
              @failed_jobs << [serialized_job, err]
            end
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

      include AdapterMethods
      include Compatibility

      ANY_QUEUE = nil

      attr_accessor :filter, :perform_immediately

      def initialize
        @queues = HashWithIndifferentAccess.new do |queues, queue_name| 

          unless queue_name.is_a?(String)|| queue_name.is_a?(Symbol)
            raise ArgumentError, "queue_name must be a Symbol or String (#{queue_name.class.name} given)"
          end

          queues[queue_name] = []
        end

        @performed_jobs = []
        @failed_jobs    = []
        @waiting_jobs   = SortedSet.new
        @filter         = ->(job) { false }

        @perform_immediately    = false
        @tracked_enqueued_jobs  = nil
        @tracked_performed_jobs = nil
      end

      def deserialize_all(jobs)
        jobs.map do |job|
          case job
          when TimeSortableJob
            deserialized = ActiveJob::Base.deserialize(job.serialized_job)
            deserialized.scheduled_at = Time.at(job.timestamp)
            deserialized
          else
            ActiveJob::Base.deserialize(job)
          end
        end
      end

      # Jobs enqueued within the given block will also be pushed onto
      # @tracked_enqueued_jobs, the contents of which are returned at
      # the end of this method.
      def enqueued_within(&block)
        jobs = nil
        begin
          @tracked_enqueued_jobs = []
          yield
          jobs = @tracked_enqueued_jobs.dup
        ensure
          @tracked_enqueued_jobs = nil
        end
        jobs
      end

      # Similar to #enqueued_within.
      def performed_within(&block)
        jobs = nil
        begin
          @tracked_performed_jobs= []
          yield
          jobs = @tracked_performed_jobs.dup
        ensure
          @tracked_performed_jobs= nil
        end
        jobs
      end

      # Jobs enqueued within this block will be performed
      # immediately.
      def perform_immediately_within(only: nil, &block)
        perform = @perform_immediately
        filter  = @filter

        begin
          @perform_immediately    = true
          @filter                 = only
          yield
        ensure
          @perform_immediately    = perform
          @filter                 = filter
        end
      end

      def queue_length(queue_name)
        @queues[queue_name].length
      end

      # This method is essentially a specialized #find_all. With no parameters,
      # it will return all serialized jobs in all queues. Given a queue name,
      # it will return just the jobs in that queue, and given a block, it will
      # yield each serialized job and return the job if that block returns true.
      def find_enqueued_jobs(queue_name=ANY_QUEUE)
        jobs = if(queue_name == ANY_QUEUE)
                 @queues.values.flatten
               else
                 @queues[queue_name]
               end

        if block_given?
          jobs.find_all do |serialized_job|
            yield serialized_job
          end
        else
          jobs
        end
      end

      # Similar to #find_enqueued_jobs, but for jobs that have been performed.
      def find_performed_jobs
        if block_given?
          @performed_jobs.find_all do |serialized_job|
            yield serialized_job
          end
        else
          @performed_jobs
        end
      end

      private
    end
  end
end
