# frozen_string_literal: true

class SidekiqPrometheus::JobMetrics
  JOB_WRAPPER_CLASS_NAME = 'ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper'
  DELAYED_CLASS_NAMES = [
    'Sidekiq::Extensions::DelayedClass',
    'Sidekiq::Extensions::DelayedModel',
    'Sidekiq::Extensions::DelayedMailer',
  ]

  def call(worker, msg, queue)
    before = GC.stat(:total_allocated_objects) if SidekiqPrometheus.gc_metrics_enabled?

    labels = { class: get_name(worker, msg), queue: queue }

    begin
      labels.merge!(custom_labels(worker))

      result = nil
      duration = Benchmark.realtime { result = yield }

      # In case the labels have changed after the worker perform method has been called
      labels.merge!(custom_labels(worker))

      registry[:sidekiq_job_duration].observe(duration, labels: labels)
      registry[:sidekiq_job_success].increment(labels: labels)

      if SidekiqPrometheus.gc_metrics_enabled?
        allocated = GC.stat(:total_allocated_objects) - before
        registry[:sidekiq_job_allocated_objects].observe(allocated, labels: labels)
      end

      result
    rescue StandardError => e
      registry[:sidekiq_job_failed].increment(labels: labels)
      raise e
    ensure
      registry[:sidekiq_job_count].increment(labels: labels)
    end
  end

  private
  
  def get_name(worker, msg)
    class_name = worker.class.to_s
    if class_name == JOB_WRAPPER_CLASS_NAME
      get_job_wrapper_name(msg)
    elsif DELAYED_CLASS_NAMES.include?(class_name)
      get_delayed_name(msg, class_name)
    else
      class_name
    end
  end
  
  def get_job_wrapper_name(msg)
    msg['wrapped']
  end

  def get_delayed_name(msg, class_name)
    # fallback to class_name since we're relying on the internal implementation
    # of the delayed extensions
    # https://github.com/mperham/sidekiq/blob/master/lib/sidekiq/extensions/class_methods.rb
    begin
      (target, method_name, _args) = YAML.load(msg['args'].first)
      if target.class == Class
        "#{target.name}##{method_name}"
      else
        "#{target.class.name}##{method_name}"
      end
    rescue
      class_name
    end
  end

  def registry
    SidekiqPrometheus
  end

  def custom_labels(worker)
    worker.respond_to?(:prometheus_labels) ? worker.prometheus_labels : {}
  end
end
