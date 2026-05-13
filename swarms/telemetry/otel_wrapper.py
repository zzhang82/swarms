import functools
import os
import time
from typing import Any, Callable, Dict, Optional

from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Initialize Telemetry
def setup_telemetry(service_name: str = "swarms"):
    # Tracing setup
    resource = Resource.create({"service.name": service_name})
    
    # Check for OTLP endpoint
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    
    if otlp_endpoint:
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        
        # Metrics setup
        metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otlp_endpoint))
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
    else:
        # Fallback or no-op if no endpoint is configured
        pass

tracer = trace.get_tracer("swarms")
meter = metrics.get_meter("swarms")

# Metrics
task_counter = meter.create_counter(
    "swarms.task.count",
    description="Number of tasks executed by agents/swarms",
)

task_duration = meter.create_histogram(
    "swarms.task.duration",
    unit="ms",
    description="Duration of task execution",
)

def trace_span(name: Optional[str] = None):
    """
    Decorator to wrap a function in an OpenTelemetry span and record metrics.
    """
    def decorator(func: Callable):
        span_name = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get metadata from 'self' if it's a method
            attributes = {}
            if args and hasattr(args[0], "__class__"):
                instance = args[0]
                attributes["swarms.class"] = instance.__class__.__name__
                if hasattr(instance, "agent_name"):
                    attributes["swarms.agent_name"] = str(instance.agent_name)
                if hasattr(instance, "model_name"):
                    attributes["swarms.model_name"] = str(instance.model_name)
            
            start_time = time.time()
            task_counter.add(1, attributes)
            
            with tracer.start_as_current_span(span_name, attributes=attributes) as span:
                try:
                    result = func(*args, **kwargs)
                    span.set_status(trace.StatusCode.OK)
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(trace.StatusCode.ERROR, str(e))
                    raise
                finally:
                    duration = (time.time() - start_time) * 1000
                    task_duration.record(duration, attributes)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            attributes = {}
            if args and hasattr(args[0], "__class__"):
                instance = args[0]
                attributes["swarms.class"] = instance.__class__.__name__
                if hasattr(instance, "agent_name"):
                    attributes["swarms.agent_name"] = str(instance.agent_name)
                if hasattr(instance, "model_name"):
                    attributes["swarms.model_name"] = str(instance.model_name)

            start_time = time.time()
            task_counter.add(1, attributes)

            with tracer.start_as_current_span(span_name, attributes=attributes) as span:
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(trace.StatusCode.OK)
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(trace.StatusCode.ERROR, str(e))
                    raise
                finally:
                    duration = (time.time() - start_time) * 1000
                    task_duration.record(duration, attributes)

        if os.path.iscoroutinefunction(func):
            return async_wrapper
        return wrapper

    return decorator
