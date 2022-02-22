//! Utilities to propagate tracing spans, e.g. from an adapter to Noria core
use std::collections::HashMap;

use opentelemetry::propagation::text_map_propagator::TextMapPropagator;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use serde::{Deserialize, Serialize};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(PartialEq, Clone, Debug, Default, Serialize, Deserialize)]
pub struct RequestContext {
    inner: HashMap<String, String>,
}

impl Injector for RequestContext {
    #[inline]
    fn set(&mut self, key: &str, value: String) {
        self.inner.insert(key.to_owned(), value);
    }
}

impl Extractor for RequestContext {
    #[inline]
    fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|k| k.as_str())
    }
    #[inline]
    fn keys(&self) -> Vec<&str> {
        self.inner.keys().map(|k| k.as_str()).collect()
    }
}

impl RequestContext {
    #[inline]
    pub fn from_current_span() -> Option<Self> {
        let span = Span::current();
        if span.is_disabled() {
            None
        } else {
            let mut ctx = RequestContext::default();
            let propagator = TraceContextPropagator::new();
            propagator.inject_context(&span.context(), &mut ctx);
            Some(ctx)
        }
    }
    #[inline]
    pub fn set_spans_parent(&self, span: &mut Span) {
        let propagator = TraceContextPropagator::new();
        let context = propagator.extract(self);
        span.set_parent(context);
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// Represents a trace-instrumented request
pub struct Instrumented<T> {
    inner: T,
    context: Option<RequestContext>,
}

impl<T> From<T> for Instrumented<T> {
    #[inline]
    fn from(inner: T) -> Self {
        let context = RequestContext::from_current_span();
        Self { inner, context }
    }
}

impl<T> Instrumented<T> {
    #[inline]
    pub fn unpack(self) -> T {
        if let Some(ctx) = self.context.as_ref() {
            ctx.set_spans_parent(&mut Span::current());
        }
        self.inner
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.context.is_some()
    }
}
