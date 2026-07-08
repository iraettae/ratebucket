# ratebucket

An async resource arbiter for APIs that enforce several rate limits at once.

A semaphore bounds concurrency. A rate limiter bounds speed. Most API clients confuse the two,
and none of them handle *two* limits at once: requests-per-minute and tokens-per-minute are
enforced together, and a permit must be granted against both or neither. `ratebucket` treats a
provider's quota as a resource to be reserved, reconciled and returned.

LLM batches are the first example, not the subject. The core does not know the word.

> **Status: pre-release.** The API is unstable until `v0.1.0`. This README is a stub; it will be
> replaced by the real thing at the tag. Nothing below is a promise yet.

## Development

This project is AI-assisted: I decide the architecture and the invariants, the model does the
routine. Where I overrode the model, the git history says so.

## License

MIT. See [LICENSE](LICENSE).
