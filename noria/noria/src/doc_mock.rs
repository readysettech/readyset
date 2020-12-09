//! This module exists entirely to work around https://github.com/rust-lang/rust/issues/65863

/// impl Discover.
///
/// https://github.com/rust-lang/rust/issues/65863
pub struct Discover<S>(std::marker::PhantomData<S>);

impl<S> tower_discover::Discover for Discover<S> {
    type Key = usize;
    type Service = S;
    type Error = tokio::io::Error;

    fn poll_discover(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<tower_discover::Change<Self::Key, Self::Service>, Self::Error>>
    {
        loop {}
    }
}

/// impl Future.
///
/// https://github.com/rust-lang/rust/issues/65863
pub struct Future<O>(std::marker::PhantomData<O>);

impl<O> std::future::Future for Future<O> {
    type Output = O;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {}
    }
}

/// impl Future.
///
/// https://github.com/rust-lang/rust/issues/65863
pub struct FutureWithExtra<O, T>(std::marker::PhantomData<O>, std::marker::PhantomData<T>);

impl<O, T> std::future::Future for FutureWithExtra<O, T> {
    type Output = O;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {}
    }
}
