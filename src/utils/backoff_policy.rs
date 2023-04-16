use {
    futures::future::{BoxFuture, FutureExt},
    std::time::Duration,
    tokio::time::sleep,
    tower::retry,
};

#[derive(Clone)]
pub struct ExponentialBackoff {
    pub attempts_remaining: usize,
    pub delay: Duration,
}

impl<Req, Res, E> retry::Policy<Req, Res, E> for ExponentialBackoff
where
    Req: Clone,
{
    type Future = BoxFuture<'static, Self>;

    fn retry(&self, _: &Req, result: Result<&Res, &E>) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(_) => (self.attempts_remaining > 0).then_some({
                let Self {
                    attempts_remaining,
                    delay,
                } = *self;

                eprintln!(
                    "timeout occurred, retrying in {}ms, {} attempts remain",
                    delay.as_millis(),
                    attempts_remaining
                );

                async move {
                    sleep(delay).await;
                    Self {
                        attempts_remaining: attempts_remaining - 1,
                        delay: delay * 2,
                    }
                }
                .boxed()
            }),
        }
    }

    fn clone_request(&self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}
